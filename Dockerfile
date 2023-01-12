#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

FROM shoothzj/compile:jdk17-mvn AS build
COPY . /opt/compile
WORKDIR /opt/compile
RUN mvn -B clean package -DskipTests

FROM shoothzj/base:jdk17

WORKDIR /opt/kafka-dashboard

COPY --from=build /opt/compile/target/kafka-dashboard-0.0.1-SNAPSHOT.jar /opt/kafka-dashboard/kafka-dashboard.jar
COPY --from=build /opt/compile/target/conf /opt/kafka-dashboard/conf
COPY --from=build /opt/compile/target/lib /opt/kafka-dashboard/lib

RUN wget -q https://github.com/paas-dashboard/kafka-dashboard-portal/releases/download/latest/kafka-dashboard-portal.tar.gz && \
    tar -xzf kafka-dashboard-portal.tar.gz && \
    rm -rf kafka-dashboard-portal.tar.gz

ENV STATIC_PATH /opt/kafka-dashboard/static/

EXPOSE 10013

CMD ["/usr/bin/dumb-init", "java", "-jar", "/opt/kafka-dashboard/kafka-dashboard.jar"]
