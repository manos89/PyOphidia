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


def reduction(cube, operation, dim, nthreads=None, ncores=None, description=None, exec_mode=None, schedule=None,
              group_size=None, missingvalue=None, grid=None, container=None, check_grid=None, concept_level=None,
              midnight=None, order=None, nmerge=None, frequency=None):

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
        for k in kwargs.keys():
            if k in method_args.keys():
                if kwargs[k] is None:
                    final_dict[k] = method_args[k]
                else:
                    final_dict[k] = kwargs[k]
        return final_dict

    def _validate_operation(operation, operations_list):
        if operation not in operations_list:
            raise RuntimeError("you have to provide with a valid operation")

    def _time_dim_validation(frequency, midnight):
        frequencies = ["s", "m", "h", "3", "6", "d", "w", "M", "q", "y", "A"]
        midnights = ["00", "24"]
        if midnight is not None:
            if midnight not in midnights:
                raise RuntimeError("Wrong midnight argument")
        if frequency not in frequencies:
            raise RuntimeError("Wrong frequency argument")

    reduce_operations = ["count", "max", "min", "avg", "sum", "std", "var", "cmoment", "acmoment", "rmoment",
                         "armoment", "quantile", "arg_max", "arg_min"]
    aggregate_operations = ["count", "max", "min", "avg", "sum"]
    aggregate_default_args = {"exec_mode": "async", "schedule": "0", "group_size": "all", "missingvalue": "NAN",
                              "grid": "-", "container": "-", "check_grid": "yes", "nthreads": "1", "ncores": "1",
                              "description": "-"}
    aggregate2_default_args = {"exec_mode": "async", "schedule": "0", "missingvalue": "NAN", "grid": "-",
                               "container": "-", "check_grid": "yes", "concept_level": "A", "midnight": "24",
                               "nthreads": "1", "ncores": "1", "description": "-"}
    merge_default_args = {"exec_mode": "async", "schedule": "0", "container": "-",
                          "nmerge": "0", "ncores": "1", "description": "-"}
    reduce_default_args = {"exec_mode": "async", "schedule": "0", "group_size": "all",
                           "missingvalue": "NAN", "grid": "-", "container": "-", "check_grid": "yes", "nthreads": "1",
                           "ncores": "1", "description": "-"}
    reduce2_default_args = {"exec_mode": "async", "schedule": "0", "missingvalue": "NAN", "grid": "-", "container": "-",
                            "check_grid": "yes", "concept_level": "A", "midnight": "24", "order": "2", "nthreads": "1",
                            "ncores": "1", "description": "-"}

    cube.info(display=False)
    kwargs = locals()
    _args_validation(cube, operation, dim)
    _dim_validation(cube, dim)
    if dim == "explicit":
        aggregate_args = _define_args(aggregate_default_args, kwargs)
        _validate_operation(operation, aggregate_operations)
        if aggregate_args["group_size"] == "all" and int(cube.nfragments) > 1:
            cube.aggregate(operation=operation, **aggregate_args)
            merge_args = _define_args(merge_default_args, kwargs)
            cube.merge(**merge_args)
            cube.aggregate(operation=operation, **aggregate_args)
        else:
            cube.aggregate(operation=operation, **aggregate_args)
    elif dim == "implicit":
        _validate_operation(operation, reduce_operations)
        reduce_args = _define_args(reduce_default_args, kwargs)
        cube.reduce(operation=operation, **reduce_args)
    else:
        if dim == _time_dimension_finder(cube):
            if kwargs["frequency"] is None:
                raise RuntimeError("you have to include frequency parameters")
            _time_dim_validation(kwargs["frequency"], kwargs["midnight"])
        else:
            if ("frequency" or "midnight") in kwargs.keys():
                import warnings
                warnings.warn("Frequency or midnight arguments will not be used")
        if _find_dim_type(cube, dim) == "implicit":
            _validate_operation(operation, reduce_operations)
            reduce2_args = _define_args(reduce2_default_args, kwargs)
            if dim == _time_dimension_finder(cube):
                reduce2_args["concept_level"] = kwargs["frequency"]
            cube.reduce2(operation=operation, dim=dim, **reduce2_args)
        else:
            _validate_operation(operation, aggregate_operations)
            aggregate2_args = _define_args(aggregate2_default_args, kwargs)
            if dim == _time_dimension_finder(cube):
                aggregate2_args["concept_level"] = kwargs["frequency"]
            cube.aggregate2(operation=operation, dim=dim, **aggregate2_args)

