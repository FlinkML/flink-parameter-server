package hu.sztaki.ilab.ps.matrix.factorization.utils

import hu.sztaki.ilab.ps.matrix.factorization.utils.Utils.{ItemId, UserId}

object InputTypes {

  /**
    * Represents an event, where we can define a timestamp
    */
  sealed abstract class  EventWithTimestamp{
    def getEventTime: Long
  }

  /**
    * Represents an event, which is either a Watermark or a custom object
    * @param time: Timestamp of the event
    * @param value: The event
    */
  case class AnyOrWatermark(time: Long, value: Any) extends Ordered[AnyOrWatermark]{
    override def compare(that: AnyOrWatermark): Int = {
      this.time.compare(that.time)
    }
  }

  /**
    * Represents an event, which can be an input for the Parameter Server
    */
  trait ParameterServerInput

  /**
    * Rating type for training data
    */
  case class Rating(user: UserId, item: ItemId, rating: Double, timestamp: Long, calculateEventTime: Long => Long = ts => ts)
    extends EventWithTimestamp with ParameterServerInput {

    def enrich(workerId: Int, ratingId: Double) = RichRating(this, workerId, ratingId)

    override def getEventTime: Long = calculateEventTime(timestamp)
  }

  /**
    * A rating with a target worker and a rating ID
    */
  case class RichRating(base: Rating, targetWorker: Int, ratingId: Double)
    extends EventWithTimestamp with ParameterServerInput {
    def reduce(): Rating = base

    override def getEventTime: Long = base.getEventTime
  }


  /**
    * Constructs a rating from a tuple
    *
    * @param t
    * A tuple containing a user ID, an item ID, and a rating
    * @return
    * A Rating with the same values and a timestamp of 0
    */
  def ratingFromTuple(t: (UserId, ItemId, Double)): Rating =
    Rating(t._1, t._2, t._3, 0)

  /**
    * Represents a request for generating negative samples for the given user
    * @param userId: Target user
    */
  case class NegativeSample(userId: UserId) extends ParameterServerInput
}