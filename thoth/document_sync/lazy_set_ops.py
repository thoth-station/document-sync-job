# thoth-document-sync
# Copyright(C) 2022 Red Hat, Inc.
#
# This program is free software: you can redistribute it and / or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""Lazy set-like operations on iterables."""

from __future__ import annotations

from typing import Iterable, TypeVar, TYPE_CHECKING

if TYPE_CHECKING:
    from _typeshed import SupportsDunderLT

    T = TypeVar("T", bound=SupportsDunderLT)


def sorted_iter_set_difference(source: Iterable[T], dest: Iterable[T]) -> Iterable[T]:
    """Compute the set difference of two sorted iterables."""
    _source = iter(source)
    _dest = iter(dest)
    d = next(_dest, None)

    for s in _source:
        while d is not None and d < s:
            d = next(_dest, None)
        if d is None:
            yield s
            break
        elif s != d:
            yield s

    yield from _source
