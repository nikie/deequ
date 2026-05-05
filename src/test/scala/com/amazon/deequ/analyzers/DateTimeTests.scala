/**
  * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"). You may not
  * use this file except in compliance with the License. A copy of the License
  * is located at
  *
  *     http://aws.amazon.com/apache2.0/
  *
  * or in the "license" file accompanying this file. This file is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  * express or implied. See the License for the specific language governing
  * permissions and limitations under the License.
  *
  */

package com.amazon.deequ.analyzers

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.analyzers.runners.NoSuchColumnException
import com.amazon.deequ.metrics.DistributionValue
import com.amazon.deequ.metrics.Distribution
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.types._
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import java.time.Instant
import scala.util.{Failure, Success}

class DateTimeTests extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  def distributionFrom(
    nonZeroValues: (Instant, Instant, DistributionValue)*): Distribution = {
    val distributionValues = nonZeroValues
      .map { case (from, to, distValue) => s"($from to $to)" -> distValue }.toMap
    Distribution(distributionValues, numberOfBins = distributionValues.keys.size)
  }

  "DateTimeDistribution analyzer" should {
    "fail for non DateType or TimestampType column" in withSparkSessionJava8APIEnabled { sparkSession =>
      val df = getDfFull(sparkSession)
      assert(DateTimeDistribution("item", DistributionInterval.HOURLY).calculate(df).value.isFailure)
    }

    "success for DateType column" in withSparkSessionJava8APIEnabled { sparkSession =>
      val df = getDfWithLocalDateAndInstant(sparkSession)
      assert(DateTimeDistribution("signupDate", DistributionInterval.MONTHLY).calculate(df).value.isSuccess)
    }

    "success for Timestamp column" in withSparkSessionJava8APIEnabled { sparkSession =>
      val df = getDfWithLocalDateAndInstant(sparkSession)
      assert(DateTimeDistribution("dateOfBirth", DistributionInterval.HOURLY).calculate(df).value.isSuccess)
    }

    "success get datetimeDistribution with Daily Interval" in withSparkSessionJava8APIEnabled { sparkSession =>
      val df = getDfWithLocalDateAndInstant(sparkSession)
      val actualDistribution = DateTimeDistribution("dateOfBirth", DistributionInterval.DAILY).calculate(df).value
      actualDistribution shouldBe Success(
        distributionFrom(
          (Instant.parse("2021-11-11T00:00:00Z"), Instant.parse("2021-11-11T23:59:59.999Z"), DistributionValue(3, 0.6)),
          (Instant.parse("2019-04-11T00:00:00Z"), Instant.parse("2019-04-11T23:59:59.999Z"), DistributionValue(2, 0.4))
        )
      )
    }

    "success get datetimeDistribution with Long Interval" in withSparkSessionJava8APIEnabled { sparkSession =>
      val df = getDfWithLocalDateAndInstant(sparkSession)
      val actualDistribution = DateTimeDistribution("dateOfBirth", 86400000L).calculate(df).value
      actualDistribution shouldBe Success(
        distributionFrom(
          (Instant.parse("2021-11-11T00:00:00Z"), Instant.parse("2021-11-11T23:59:59.999Z"), DistributionValue(3, 0.6)),
          (Instant.parse("2019-04-11T00:00:00Z"), Instant.parse("2019-04-11T23:59:59.999Z"), DistributionValue(2, 0.4))
        )
      )
    }
  }

  "MinimumDateTime analyzer" should {
    "fail for non DateType or TimestampType column" in withSparkSessionJava8APIEnabled { sparkSession =>
      val df = getDfFull(sparkSession)
      assert(MinimumDateTime("item").calculate(df).value.isFailure)
    }

    "success for DateType column" in withSparkSessionJava8APIEnabled { sparkSession =>
      val df = getDfWithLocalDateAndInstant(sparkSession)
      assert(MinimumDateTime("signupDate").calculate(df).value.isSuccess)
    }

    "success for Timestamp column" in withSparkSessionJava8APIEnabled { sparkSession =>
      val df = getDfWithLocalDateAndInstant(sparkSession)
      MinimumDateTime("dateOfBirth").calculate(df).value shouldBe Success(Instant.parse("2019-04-11T12:15:00Z"))
    }
  }

  "MaximumDateTime analyzer" should {
    "fail for non DateType or TimestampType column" in withSparkSessionJava8APIEnabled { sparkSession =>
      val df = getDfFull(sparkSession)
      assert(MaximumDateTime("item").calculate(df).value.isFailure)
    }

    "success for DateType column" in withSparkSessionJava8APIEnabled { sparkSession =>
      val df = getDfWithLocalDateAndInstant(sparkSession)
      assert(MaximumDateTime("signupDate").calculate(df).value.isSuccess)
    }

    "success for Timestamp column" in withSparkSessionJava8APIEnabled { sparkSession =>
      val df = getDfWithLocalDateAndInstant(sparkSession)
      MaximumDateTime("dateOfBirth").calculate(df).value shouldBe Success(Instant.parse("2021-11-11T09:15:00Z"))
    }
  }
}
