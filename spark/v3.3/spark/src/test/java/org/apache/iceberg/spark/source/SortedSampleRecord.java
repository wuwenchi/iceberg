/*
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
 */

package org.apache.iceberg.spark.source;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Objects;

public class SortedSampleRecord {
  private Long c1;
  private Double c2;
  private Float c3;
  private Integer c4;
  private Short c5;
  private String c6;
  private Date c7;
  private Timestamp c8;
  private BigDecimal c9;
  public SortedSampleRecord() {
  }

  public SortedSampleRecord(
      Long c1,
      Double c2,
      Float c3,
      Integer c4,
      Short c5,
      String c6,
      Date c7,
      Timestamp c8,
      BigDecimal c9) {
    this.c1 = c1;
    this.c2 = c2;
    this.c3 = c3;
    this.c4 = c4;
    this.c5 = c5;
    this.c6 = c6;
    this.c7 = c7;
    this.c8 = c8;
    this.c9 = c9;
  }

  @Override
  public String toString() {
    return "SortedSampleRecord{" +
        "c1=" + c1 +
        ", c2=" + c2 +
        ", c3=" + c3 +
        ", c4=" + c4 +
        ", c5=" + c5 +
        ", c6='" + c6 + '\'' +
        ", c7=" + c7 +
        ", c8=" + c8 +
        ", c9=" + c9 +
        '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(c1, c2, c3, c4, c5, c6, c7, c8, c9);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SortedSampleRecord that = (SortedSampleRecord) o;
    return Objects.equals(c1, that.c1) && Objects.equals(c2, that.c2) &&
        Objects.equals(c3, that.c3) && Objects.equals(c4, that.c4) &&
        Objects.equals(c5, that.c5) && Objects.equals(c6, that.c6) &&
        Objects.equals(c7, that.c7) && Objects.equals(c8, that.c8) &&
        Objects.equals(c9, that.c9);
  }

  public Long getC1() {
    return c1;
  }

  public void setC1(Long c1) {
    this.c1 = c1;
  }

  public Double getC2() {
    return c2;
  }

  public void setC2(Double c2) {
    this.c2 = c2;
  }

  public Float getC3() {
    return c3;
  }

  public void setC3(Float c3) {
    this.c3 = c3;
  }

  public Integer getC4() {
    return c4;
  }

  public void setC4(Integer c4) {
    this.c4 = c4;
  }

  public Short getC5() {
    return c5;
  }

  public void setC5(Short c5) {
    this.c5 = c5;
  }

  public String getC6() {
    return c6;
  }

  public void setC6(String c6) {
    this.c6 = c6;
  }

  public Date getC7() {
    return c7;
  }

  public void setC7(Date c7) {
    this.c7 = c7;
  }

  public Timestamp getC8() {
    return c8;
  }

  public void setC8(Timestamp c8) {
    this.c8 = c8;
  }

  public BigDecimal getC9() {
    return c9;
  }

  public void setC9(BigDecimal c9) {
    this.c9 = c9;
  }
}
