/*
 *    Copyright 2018 Zhang Di
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.dizhang.pubg
import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema

class ReportDeserializer extends AbstractDeserializationSchema[Report] {
  def deserialize(message: Array[Byte]): Report = {
    val tmp = new java.lang.String(message, StandardCharsets.UTF_8)
    val s = tmp.split(",")
    Report(s(0), s(1), s(2), s(3).toLong)
  }
}
