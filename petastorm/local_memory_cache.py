#  Copyright (c) 2017-2018 Uber Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import division

from petastorm.cache import CacheBase


class LocalMemoryCache(CacheBase):
    def __init__(self, size_limit_bytes, cleanup=False):
        """LocalMemoryCache is an adapter to a diskcache implementation.

        LocalMemoryCache can be used by a petastorm Reader class to temporarily keep parts of the dataset in local memory.

        :param size_limit_bytes: Maximal size of the memory to be used by cache. The size of the cache may actually
                                 grow somewhat above the size_limit_bytes, so the limit is not very strict.
        :param cleanup: If set to True, cache directory would be removed when cleanup() method is called.
        """

        self._cleanup = cleanup
        self._cache = dict()

    def get(self, key, fill_cache_func):
        value = self._cache.get(key)
        if value is None:
            value = fill_cache_func()
            self._cache[key] = value

        return value

    def cleanup(self):
        if self._cleanup:
            self._cache.clear()
