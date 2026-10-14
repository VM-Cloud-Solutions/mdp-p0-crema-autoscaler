#!/bin/bash
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

# Start the scaler server in the background
java -jar /app/scaler_server.jar &

# Wait for the Java gRPC server to be fully listening on port 50051
echo "Waiting For gRPC on port 50051..."
until (echo > /dev/tcp/127.0.0.1/50051) >/dev/null 2>&1; do
  sleep 1
done

# Start the metric-provider in the foreground
/app/metric-provider
