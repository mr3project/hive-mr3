/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.storage.jdbc.spitter;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

public class DecimalIntervalSplitter implements IntervalSplitter {
  @Override
  public List<MutablePair<String, String>> getIntervals(String lowerBound, String upperBound, int numPartitions, TypeInfo
          typeInfo) {
    List<MutablePair<String, String>> intervals = new ArrayList<>();
    DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo)typeInfo;
    int scale = decimalTypeInfo.getScale();
    BigDecimal decimalLower = new BigDecimal(lowerBound);
    BigDecimal decimalUpper = new BigDecimal(upperBound);
    BigDecimal decimalInterval = (decimalUpper.subtract(decimalLower)).divide(new BigDecimal(numPartitions),
            MathContext.DECIMAL64);
    BigDecimal splitDecimalLower, splitDecimalUpper;
    for (int i=0;i<numPartitions;i++) {
      splitDecimalLower = decimalLower.add(decimalInterval.multiply(new BigDecimal(i))).setScale(scale,
              RoundingMode.HALF_EVEN);
      splitDecimalUpper = decimalLower.add(decimalInterval.multiply(new BigDecimal(i+1))).setScale(scale,
              RoundingMode.HALF_EVEN);
      if (splitDecimalLower.compareTo(splitDecimalUpper) < 0) {
        intervals.add(new MutablePair<String, String>(splitDecimalLower.toPlainString(), splitDecimalUpper.toPlainString()));
      }
    }
    return intervals;
  }
}
