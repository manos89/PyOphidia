#
#     PyOphidia - Python bindings for Ophidia
#     Copyright (C) 2015-2020 CMCC Foundation
#
#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
#
#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
#
#     You should have received a copy of the GNU General Public License
#     along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import sys
import os
import base64
import struct
import PyOphidia.cube as cube
from inspect import currentframe

sys.path.append(os.path.dirname(__file__))


def get_linenumber():
    cf = currentframe()
    return __file__, cf.f_back.f_lineno


def reduction(cube, operation, dim, nthreads=1, ncores=1, description="-", **kwargs):

    def _time_dimension_finder(cube):
        """
        _time_dimension_finder(cube) -> str: finds the time dimension, if any
        :param cube: the cube object
        :type cube:  <class 'PyOphidia.cube.Cube'>
        :returns: str|None
        :rtype: <class 'str'>
        """
        for c in cube.dim_info:
            if c["hierarchy"].lower() == "oph_time":
                return c["name"]
        return None

    def _args_validation(cube, operation, dim):
        if not cube or not operation or not dim:
            raise RuntimeError('You have to declare cube and operation and dim')

    def _dim_validation(cube, dim):
        all_dims = [d["name"] for d in cube.dim_info]
        if dim not in (all_dims) and dim != "explicit" and dim != "implicit":
            raise RuntimeError("dim doesn't have a valid value")

    def _find_dim_type(cube, dim):
        array_bool = [d["array"] for d in cube.dim_info if d["name"] == dim][0]
        if array_bool == "no":
            return "implicit"
        else:
            return "explicit"

    def _define_args(method_args, kwargs):
        final_dict = {}
        for k in method_args:
            if k in kwargs.keys():
                final_dict[k] = kwargs[k]
            else:
                final_dict[k] = method_args[k]
        return final_dict

    def _validate_operation(operation, operations_list):
        if operation not in operations_list:
            raise RuntimeError("you have to provide with a valid operation")

    def _time_dim_validation(frequency, midnight):
        frequencies = ["s", "m", "h", "3", "6", "d", "w", "M", "q", "y", "A"]
        midnights = ["00", "24"]
        if midnight not in midnights:
            raise RuntimeError("Wrong midnight argument")
        if frequency not in frequencies:
            raise RuntimeError("Wrong frequency argument")

    reduce_operations = ["count", "max", "min", "avg", "sum", "std", "var", "cmoment", "acmoment", "rmoment",
                         "armoment", "quantile", "arg_max", "arg_min"]
    aggregate_operations = ["count", "max", "min", "avg", "sum"]
    aggregate_default_args = {"exec_mode": "async", "schedule": "0", "group_size": "all", "missingvalue": "NAN",
                              "grid": "-", "container": "-", "check_grid": "yes"}
    aggregate2_default_args = {"exec_mode": "async", "schedule": "0", "missingvalue": "NAN", "grid": "-",
                               "container": "-", "check_grid": "yes", "concept_level": "A", "midnight": "24"}
    merge_default_args = {"exec_mode": "async", "schedule": "0", "grid": "-", "container": "-",
                          "nmerge": "0"}
    reduce_default_args = {"exec_mode": "async", "schedule": "0", "group_size": "all",
                           "missingvalue": "NAN", "grid": "-", "container": "-", "check_grid": "yes",
                           }
    reduce2_default_args = {"exec_mode": "async", "schedule": "0", "missingvalue": "NAN", "grid": "-", "container": "-",
                            "check_grid": "yes", "concept_level": "A", "midnight": "24", "order": "2"}

    cube.info(display=False)
    _args_validation(cube, operation, dim)
    _dim_validation(cube, dim)
    if dim == "explicit":
        aggregate_args = _define_args(aggregate_default_args, kwargs)
        _validate_operation(operation, aggregate_operations)
        if aggregate_args["group_size"] == "all":
            cube.aggregate(operation=operation, ncores=ncores, nthreads=nthreads, description=description,
                           **aggregate_args)
            merge_args = _define_args(merge_default_args, kwargs)
            cube.merge(description=description, ncores=ncores, **merge_args)
            cube.aggregate(operation=operation, ncores=ncores, nthreads=nthreads, description=description,
                           **aggregate_args)
        else:
            cube.aggregate(operation=operation, ncores=ncores, nthreads=nthreads, description=description,
                           **aggregate_args)
    elif dim == "implicit":
        _validate_operation(operation, reduce_operations)
        reduce_args = _define_args(reduce_default_args, kwargs)
        cube.reduce(operation=operation, ncores=ncores, nthreads=nthreads, description=description, **reduce_args)
    else:

        if dim == _time_dimension_finder(cube):
            if ("frequency" or "midnight") not in kwargs.keys():
                raise RuntimeError("you have to include frequency and midnight parameters")
            _time_dim_validation(kwargs["frequency"], kwargs["midnight"])
        else:
            if ("frequency" or "midnight") in kwargs.keys():
                import warnings
                warnings.warn("Frequency or midnight arguments will not be used")
        if _find_dim_type(cube, dim) == "implicit":
            _validate_operation(operation, reduce_operations)
            reduce2_args = _define_args(reduce2_default_args, kwargs)
            cube.reduce2(operation=operation, dim=dim, ncores=ncores, nthreads=nthreads, description=description,
                         **reduce2_args)
        else:
            _validate_operation(operation, aggregate_operations)
            aggregate2_args = _define_args(aggregate2_default_args, kwargs)
            cube.aggregate2(operation=operation, dim=dim, ncores=ncores, nthreads=nthreads, description=description,
                            **aggregate2_args)


