package example.intelligentTraffic

/**
 * 四次排序自定义key类
 * @param lowSpeedCount 低速车辆的数量
 * @param normalSpeedCount 正常速度车辆数量
 * @param mediumSpeedCount 中速车辆数量
 * @param highSpeedCount 高速车辆数量
 */
class SpeedSortKey(val lowSpeedCount:Int,val normalSpeedCount:Int,val
mediumSpeedCount:Int,val highSpeedCount:Int)
  extends Ordered[SpeedSortKey] with Serializable {

  /**
   * 实现compare()方法
   */
  override def compare(that: SpeedSortKey): Int = {
    //排序规则：高速车辆数量多的卡口排在前面，若高速车辆数量相同则比较中速车辆数量，依次类推。若第highSpeedCount字段不相等，按照highSpeedCount字段升序(默认)排列，可以在排序时调用sortByKey(false)进行降序排列，false代表降序
    if (this.highSpeedCount - that.highSpeedCount != 0) {
      this.highSpeedCount - that.highSpeedCount
    }else if (this.mediumSpeedCount - that.mediumSpeedCount!=0) {
      this.mediumSpeedCount - that.mediumSpeedCount
    }else if (this.normalSpeedCount - that.normalSpeedCount!=0) {
      this.normalSpeedCount - that.normalSpeedCount
    }else if (this.lowSpeedCount - that.lowSpeedCount!=0) {
      this.lowSpeedCount - that.lowSpeedCount
    }else{
      0
    }

  }

  /**
   * 重写toString，便于查看结果
   * 直接输出该类的实例时，默认调用toString方法
   */
  override def toString: String = {
    "SpeedSortKey [lowSpeedCount=" + lowSpeedCount +
      ", normalSpeedCount=" + normalSpeedCount +
      ", mediumSpeedCount=" + mediumSpeedCount +
      ", highSpeedCount=" + highSpeedCount + "]"
  }
}