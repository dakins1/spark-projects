package customML

object BasicStats {
  def mean(x: Seq[Double]): Double = x.sum / x.length

  def variance(x: Seq[Double]): Double = {
    val avg = mean(x)
    val avgSqrd = x.map(r => r*r).sum / x.length
    avgSqrd - (avg*avg)
  }

  def stdev(x: Seq[Double]): Double = math.sqrt(variance(x))

  def covariance(x: Seq[Double], y: Seq[Double]): Double = {
    val xyAvg = mean(x.zip(y).map(p => p._1*p._2))
    xyAvg - (mean(x) * mean(y))
  }

  def correlation(x: Seq[Double], y: Seq[Double]): Double = {
        covariance(x, y) / (math.sqrt(variance(x) * variance(y)))
  }

  def weightedMean(x: Seq[Double], weight: Double => Double): Double = {
    val numer =  x.map(v => weight(v)*v).sum
    val denom = x.map(xi => weight(xi)).sum
    numer / denom
  }
}