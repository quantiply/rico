/*
 * Copyright 2014-2015 Quantiply Corporation. All rights reserved.
 *
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
package com.quantiply.samza;

import org.apache.samza.task.ClosableTask;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.WindowableTask;

// Combine all task interface so that it is easy to implement in Jython.
// There might be a way to avoid this but this is simpler to understand and works perfectly.
public interface PyTask extends StreamTask, InitableTask, WindowableTask, ClosableTask {}
