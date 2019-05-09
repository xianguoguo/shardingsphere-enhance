/*
 * Copyright 2016-2018 shardingsphere.io.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingsphere.core.routing.type.complex;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import io.shardingsphere.core.exception.ShardingException;
import io.shardingsphere.core.optimizer.condition.ShardingConditions;
import io.shardingsphere.core.routing.type.RoutingEngine;
import io.shardingsphere.core.routing.type.RoutingResult;
import io.shardingsphere.core.routing.type.RoutingTable;
import io.shardingsphere.core.routing.type.TableUnit;
import io.shardingsphere.core.routing.type.standard.StandardRoutingEngine;
import io.shardingsphere.core.rule.BindingTableRule;
import io.shardingsphere.core.rule.ShardingRule;
import io.shardingsphere.core.rule.TableRule;
import io.shardingsphere.core.routing.router.ShardingConfiguration;

import java.util.*;


/**
 * Complex routing engine.
 *
 * @author gaohongtao
 * @author zhangliang
 */
public final class YComplexRoutingEngine implements RoutingEngine {
  private final ShardingRule shardingRule;

  private final Collection<String> logicTables;

  private final ShardingConditions shardingConditions;

  private final boolean dqlStatement;

  public YComplexRoutingEngine(ShardingRule shardingRule,
      Collection<String> logicTables,
      ShardingConditions shardingConditions, boolean dqlStatement) {
    this.shardingRule = shardingRule;
    this.logicTables = logicTables;
    this.shardingConditions = shardingConditions;
    this.dqlStatement = dqlStatement;
  }

  @Override
  public RoutingResult route() {
    Collection<RoutingResult> result = new ArrayList<>(logicTables.size());
    Collection<String> bindingTableNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    for (String each : logicTables) {
      Optional<TableRule> tableRule = shardingRule.findTableRuleByLogicTable(each);
      if (tableRule.isPresent()) {
        if (!bindingTableNames.contains(each)) {
          result.add(new StandardRoutingEngine(shardingRule, tableRule.get().getLogicTable(), shardingConditions).route());
        }
        Optional<BindingTableRule> bindingTableRule = shardingRule.findBindingTableRule(each);
        if (bindingTableRule.isPresent()) {
          bindingTableNames.addAll(Lists.transform(bindingTableRule.get().getTableRules(), new Function<TableRule, String>() {

            @Override
            public String apply(final TableRule input) {
              return input.getLogicTable();
            }
          }));
        }
      }
    }
    if (result.isEmpty()) {
      throw new ShardingException("Cannot find table rule and default data source with logic tables: '%s'", logicTables);
    }
    RoutingResult routingResult;
    if (1 == result.size()) {
      routingResult = result.iterator().next();
    } else {
      routingResult = new CartesianRoutingEngine(result).route();
    }
    revise(routingResult);
    return routingResult;
  }

  /**
   * 对路由结果进行修正
   * 读老或者双写
   * @param routingResult
   */
  private void revise(RoutingResult routingResult) {
    Collection<TableUnit> tableUnits = routingResult.getTableUnits().getTableUnits();
    Map<String, TableUnit> revisedTableUnitMap = new HashMap<>();
    // 如果读老，则需要删除部分TableUnit。如果双写，则无需删除。
    Iterator<TableUnit> tableUnitIterator = tableUnits.iterator();
    while (tableUnitIterator.hasNext()) {
      TableUnit tableUnit = tableUnitIterator.next();
      List<RoutingTable> routingTableList = tableUnit.getRoutingTables();
      final StringBuilder stringBuilder = new StringBuilder();
      Boolean readOld = false;   // 该tableUnit是否需要读老。默认不读老。
      boolean doubleWrite = false;   // 该tableUnit是否需要双写。默认不双写。
      List<RoutingTable> revisedTableList = new ArrayList<>();
      for(RoutingTable routingTable : routingTableList) {
        String logicTableName = routingTable.getLogicTableName();
        stringBuilder.append(stringBuilder.length() != 0 ? "-" : "");
        //logicTableName需要读老或者双写 TODO
        if (dqlStatement && ShardingConfiguration.isReadOld(logicTableName)) {
          stringBuilder.append(routingTable.getLogicTableName());
          // 读老
          readOld = true;
          // 读老，逻辑表与物理表名称相同
          revisedTableList.add(new RoutingTable(logicTableName, logicTableName));
        }
        else if(!dqlStatement && ShardingConfiguration.isDoubleModify(logicTableName)) {
          stringBuilder.append(routingTable.getLogicTableName());
          // 双写
          doubleWrite = true;
          // 双写，逻辑表与物理表名称相同
          revisedTableList.add(new RoutingTable(logicTableName, logicTableName));
        }
        else {
          stringBuilder.append(routingTable.getActualTableName());
          revisedTableList.add(new RoutingTable(logicTableName, routingTable.getActualTableName()));
        }
      }
      String tableUnitSummaryStr = stringBuilder.toString();
      if(readOld || doubleWrite) {
        if(!revisedTableUnitMap.containsKey(tableUnitSummaryStr)) {
          TableUnit revisedTableUnit = new TableUnit(shardingRule.getShardingRuleConfig().getDefaultDataSourceName());
          revisedTableUnit.getRoutingTables().addAll(revisedTableList);
          revisedTableUnitMap.put(tableUnitSummaryStr, revisedTableUnit);
        }
        // 读老，则删除该分片表单元
        if(readOld) {
          tableUnitIterator.remove();
        }
      }
    }
    Iterator<TableUnit> revisedTableUnitIterator = revisedTableUnitMap.values().iterator();
    // 删除双写结果中，已在原分片结果中已存在的。
    while (revisedTableUnitIterator.hasNext()) {
      TableUnit revisedTableUnit = revisedTableUnitIterator.next();
      for(TableUnit tableUnit : tableUnits) {
        if(tableUnitEquals(tableUnit, revisedTableUnit)) {
          revisedTableUnitIterator.remove();
        }
      }
    }
    // 将双写或读老的sql路由加入到表单元当中
    tableUnits.addAll(revisedTableUnitMap.values());
  }

  /**
   * 根据读老或双写策略，对表单元生成唯一分片字符串。
   * @param tableUnit
   * @return
   */
  private String tableUnitSummary(TableUnit tableUnit) {
    List<RoutingTable> routingTableList = tableUnit.getRoutingTables();
    final StringBuilder stringBuilder = new StringBuilder();
    routingTableList.forEach((routingTable) -> {
      String logicTableName = routingTable.getLogicTableName();
      stringBuilder.append(stringBuilder.length() != 0 ? "-" : "");
      //logicTableName需要读老或者双写 TODO
      if (true) {
        stringBuilder.append(routingTable.getLogicTableName());
      } else {
        stringBuilder.append(routingTable.getActualTableName());
      }
    });
    return stringBuilder.toString();
  }

  /**
   * 判断两个tableUnit是否相同
   * @param tableUnit1
   * @param tableUnit2
   * @return
   */
  private boolean tableUnitEquals(TableUnit tableUnit1, TableUnit tableUnit2) {
    if(!tableUnit1.getDataSourceName().equals(tableUnit2.getDataSourceName())) {
      return false;
    }
    if(tableUnit1.getRoutingTables().size() != tableUnit2.getRoutingTables().size()) {
      return false;
    }
    Iterator<RoutingTable> routingTableIterator1 = tableUnit1.getRoutingTables().iterator();
    Iterator<RoutingTable> routingTableIterator2 = tableUnit2.getRoutingTables().iterator();
    while (routingTableIterator1.hasNext()) {
      RoutingTable routingTable1 = routingTableIterator1.next();
      RoutingTable routingTable2 = routingTableIterator2.next();
      if(!routingTable1.getLogicTableName().equals(routingTable2.getActualTableName()) || !routingTable1.getActualTableName().equals(routingTable2.getActualTableName())) {
        return false;
      }
    }
    return true;
  }
}
