package io.shardingsphere.core.routing.router;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import net.openhft.compiler.CompilerUtils;

public class ShardingEnginePreload {

  /**
   * 预加载RoutingEngineFactory
   * 需要保证RoutingEngineFactory在官方的sharding-core中同名类之前预先加载
   * 在依赖该模块工程的main方法中，首先调用该方法即可。
   */
  public static void preLoad() {
    load("io.shardingsphere.core.routing.router.sharding.RoutingEngineFactory", "shardingEnhancer/RoutingEngineFactory.class");
  }
  private static void load(String className, String fileName) {
    try {
      InputStream inputStream = ShardingEnginePreload.class.getClassLoader()
          .getResourceAsStream(fileName);
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      byte[] buffer = new byte[4096];
      int n = 0;
      while ((n = inputStream.read(buffer)) != -1) {
        byteArrayOutputStream.write(buffer, 0, n);
      }
      inputStream.close();
      byte[] bytes = byteArrayOutputStream.toByteArray();
      Class clazz = CompilerUtils
          .defineClass(ShardingEnginePreload.class.getClassLoader(), className, bytes);
      clazz.newInstance();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
}
