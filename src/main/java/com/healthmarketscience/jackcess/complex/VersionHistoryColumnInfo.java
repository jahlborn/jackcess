/*
Copyright (c) 2013 James Ahlborn

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.healthmarketscience.jackcess.complex;

/**
 * Complex column info for a column which tracking the version history of an
 * "append only" memo column.
 * <p>
 * Note, the strongly typed update/delete methods are <i>not</i> supported for
 * version history columns (the data is supposed to be immutable).  That said,
 * the "raw" update/delete methods are supported for those that <i>really</i>
 * want to muck with the version history data.
 *
 * @author James Ahlborn
 */
public interface VersionHistoryColumnInfo extends ComplexColumnInfo<Version>
{

}
