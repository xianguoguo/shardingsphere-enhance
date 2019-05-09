package io.shardingsphere.core.routing.router;

import java.util.HashMap;
import java.util.Map;

public class ShardingConfiguration {

  // 无需volatile修饰
  public static Map<String, Boolean> doubleWriteMap = new HashMap<>();
  public static Map<String, Boolean> readOldMap = new HashMap<>();

  public static boolean isDoubleModify(String tableName) {
    return doubleWriteMap.getOrDefault(tableName, true);
  }

  /**
   * 更新读老配置
   * @param readOldMap
   */
  public static void setReadOldMap(Map<String, Boolean> readOldMap) {
    ShardingConfiguration.readOldMap = readOldMap;
  }

  /**
   * 更新双写配置
   * @param doubleWriteMap
   */
  public static void setDoubleModifyMap(Map<String, Boolean> doubleWriteMap) {
    ShardingConfiguration.doubleWriteMap = doubleWriteMap;
  }

  /**
   * 如果未找到相关配置，默认从原库原表读取
   * @param tableName
   * @return
   */
  public static boolean isReadOld(String tableName) {
    return readOldMap.getOrDefault(tableName, true);
  }

}
