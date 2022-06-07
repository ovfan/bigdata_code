package com.fanfan.gmall.realtime.util

import java.util.ResourceBundle

/**
 * 配置文件解析
 */
object MyPropsUtils {
  // ResourceBundle可以从配置文件中解析k=v的 value值
  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")  // config.properties
  def apply(key:String): String = {
    bundle.getString(key)
  }

  // 测试
  def main(args: Array[String]): Unit = {
    println(MyPropsUtils("a"))
  }
}
