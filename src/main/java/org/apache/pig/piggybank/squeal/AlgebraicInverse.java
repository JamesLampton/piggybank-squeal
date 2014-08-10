/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
 * A companion interface for Algebraic.  By providing an inverse
 * initializer we can negate values in a streaming environment.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface AlgebraicInverse{
    
    /**
     * Get the initial inverse function. 
     * @return A function name of f_init. f_init should be an eval func.
     * The return type of f_init.exec() has to be Tuple
     */
    public String getInitialInverse();
}
