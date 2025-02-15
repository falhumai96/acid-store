/*
 * Copyright 2019-2021 Wren Powell
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

use std::collections::hash_map;
use std::iter::{ExactSizeIterator, FusedIterator};

use crate::repo::state::ObjectKey;

/// An iterator over the keys in a [`ValueRepo`].
///
/// This value is created by [`ValueRepo::keys`].
///
/// [`ValueRepo`]: crate::repo::value::ValueRepo
/// [`ValueRepo::keys`]: crate::repo::value::ValueRepo::keys
#[derive(Debug, Clone)]
pub struct Keys<'a, K>(pub(super) hash_map::Keys<'a, K, ObjectKey>);

impl<'a, K> Iterator for Keys<'a, K> {
    type Item = &'a K;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<'a, K> FusedIterator for Keys<'a, K> {}

impl<'a, K> ExactSizeIterator for Keys<'a, K> {}
