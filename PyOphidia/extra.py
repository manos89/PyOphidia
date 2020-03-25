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


def convert_to_dataframe(cube):
    """convert_to_pandas(cube=cube) -> pandas.core.frame.DataFrame : convert a Pyophidia.cube to pandas dataframe
    :param cube: the initial cube
    :type cube: <class 'PyOphidia.cube.Cube'>
    :returns: a pandas.core.frame.DataFrame object
    :rtype: <class 'pandas.core.frame.DataFrame'>
    :raises: RuntimeError
    """

    def _dependency_check():
        """_dependency_check() -> checks for xarray dependency in user's system
        :returns: NoneType
        :rtype: <class 'NoneType'>
        :raises: RuntimeError
        """
        try:
            import pandas
        except ModuleNotFoundError:
            raise RuntimeError('pandas is not installed')

    def _time_dimension_finder(cube):
        """
        _time_dimension_finder(cube) -> str: finds the time dimension, if any
        :param cube: the cube object
        :type cube:  <class 'PyOphidia.cube.Cube'>
        :returns: str|None
        :rtype: <class 'str'>
        """
        for c in cube.dim_info:
            if c["type"].lower() == "oph_time":
                return c["name"]
        return None

    def _get_unpack_format(element_num, output_type):
        if output_type == 'float':
            format = str(element_num) + 'f'
        elif output_type == 'double':
            format = str(element_num) + 'd'
        elif output_type == 'int':
            format = str(element_num) + 'i'
        elif output_type == 'long':
            format = str(element_num) + 'l'
        elif output_type == 'short':
            format = str(element_num) + 'h'
        elif output_type == 'char':
            format = str(element_num) + 'c'
        else:
            raise RuntimeError('The value type is not valid')
        return format

    def _calculate_decoded_length(decoded_string, output_type):
        if output_type == 'float' or output_type == 'int':
            num = int(float(len(decoded_string)) / float(4))
        elif output_type == 'double' or output_type == 'long':
            num = int(float(len(decoded_string)) / float(8))
        elif output_type == 'short':
            num = int(float(len(decoded_string)) / float(2))
        elif output_type == 'char':
            num = int(float(len(decoded_string)) / float(1))
        else:
            raise RuntimeError('The value type is not valid')
        return num

    def _add_coordinates(cube, df, response):
        """
        _add_coordinates(cube, dr, response) -> pandas.core.frame.DataFrame,int: a function that uses the response from
            the oph_explorecube and adds coordinates to the DataFrame object
        :param cube: the cube object
        :type cube:  <class 'PyOphidia.cube.Cube'>
        :param df: the pandas dataframe object
        :type df:  <class 'pandas.core.frame.DataFrame'>
        :param response: response from pyophidia query
        :type response:  <class 'dict'>
        :returns: pandas.core.frame.DataFrame,int|None
        :rtype: <class 'pandas.core.frame.DataFrame'>,<class 'int'>|None
        """
        max_size = max([int(dim["size"]) for dim in cube.dim_info])
        lengths = []
        try:
            for response_i in response['response']:
                if response_i['objkey'] == 'explorecube_dimvalues':
                    for response_j in response_i['objcontent']:
                        if response_j['title'] and response_j['rowfieldtypes'] and response_j['rowfieldtypes'][1] and \
                                response_j['rowvalues']:
                            if response_j['title'] == _time_dimension_finder(cube):
                                temp_array = []
                                for val in response_j['rowvalues']:
                                    dims = [s.strip() for s in val[1].split(',')]
                                    temp_array.append(dims[0])
                                df[response_j['title']] = temp_array
                            else:
                                lengths.append(len(response_j['rowvalues']))
                                temp_array = []
                                for val in response_j['rowvalues']:
                                    decoded_bin = base64.b64decode(val[1])
                                    length = _calculate_decoded_length(decoded_bin, response_j['rowfieldtypes'][1])
                                    format = _get_unpack_format(length, response_j['rowfieldtypes'][1])
                                    dims = struct.unpack(format, decoded_bin)
                                    temp_array.append(dims[0])
                                if max_size > len(list(temp_array)):
                                    for i in range(0, max_size - len(list(temp_array))):
                                        temp_array.append(None)
                                df[response_j['title']] = list(temp_array)
                        else:
                            raise RuntimeError("Unable to get dimension name or values in response")
                    break
        except Exception as e:
            print(get_linenumber(), "Unable to get dimensions from response:", e)
            return None
        return df, lengths

    def _add_measure(cube, df, response, lengths):
        """
        _add_measure(cube, dr, response) -> pandas.core.frame.DataFrame: a function that uses the response from
            the oph_explorecube and adds the measure to the dataframe object
        :param cube: the cube object
        :type cube:  <class 'PyOphidia.cube.Cube'>
        :param df: the pandas dataframe object
        :type df:  <class 'pandas.core.frame.DataFrame'>
        :param response: response from pyophidia query
        :type response:  <class 'dict'>
        :param lengths: list of the coordinate lengths
        :type lengths:  <class 'list'>
        :returns: pandas.core.frame.DataFrame|None
        :rtype: <class 'pandas.core.frame.DataFrame'>|None
        """
        try:
            for response_i in response['response']:
                if response_i['objkey'] == 'explorecube_data':
                    for response_j in response_i['objcontent']:
                        if response_j['title'] and response_j['rowkeys'] and response_j['rowfieldtypes'] \
                                and response_j['rowvalues']:
                            measure_index = 0
                            for i, t in enumerate(response_j['rowkeys']):
                                if response_j['title'] == t:
                                    measure_index = i
                                    break
                            if measure_index == 0:
                                raise RuntimeError("Unable to get measure name in response")
                            values = []
                            for val in response_j['rowvalues']:
                                decoded_bin = base64.b64decode(val[measure_index])
                                length = _calculate_decoded_length(decoded_bin,
                                                                   response_j['rowfieldtypes'][measure_index])
                                format = _get_unpack_format(length, response_j['rowfieldtypes'][measure_index])
                                measure = struct.unpack(format, decoded_bin)
                                if (type(measure)) is (tuple or list) and len(measure) == 1:
                                    values.append(measure[0])
                                else:
                                    for v in measure:
                                        values.append(v)
                            for i in range(len(lengths) - 1, -1, -1):
                                current_array = []
                                if i == len(lengths) - 1:
                                    for j in range(0, len(values), lengths[i]):
                                        current_array.append(values[j:j + lengths[i]])
                                else:
                                    for j in range(0, len(previous_array), lengths[i]):
                                        current_array.append(previous_array[j:j + lengths[i]])
                                previous_array = current_array
                            measure = previous_array[0]
                        else:
                            raise RuntimeError("Unable to get measure values in response")
                        break
                    break
            if len(measure) == 0:
                raise RuntimeError("No measure found")
        except Exception as e:
            print("Unable to get measure from response:", e)
            return None
        sorted_coordinates = []
        for l in lengths:
            for c in cube.dim_info:
                if l == int(c["size"]) and c["name"] not in sorted_coordinates:
                    sorted_coordinates.append(c["name"])
                    break
        df[cube.measure] = measure
        return df

    _dependency_check()
    import pandas as pd
    cube.info(display=False)
    pid = cube.pid
    df = pd.DataFrame()
    query = 'oph_explorecube ncore=1;base64=yes;level=2;show_index=yes;subset_type=coord;limit_filter=0;cube={0};'. \
        format(pid)
    cube.client.submit(query, display=False)
    response = cube.client.deserialize_response()
    try:
        df, lengths = _add_coordinates(cube, df, response)
    except Exception as e:
        print(get_linenumber(), "Something is wrong with the coordinates, error: ", e)
        return None
    try:
        df = _add_measure(cube, df, response, lengths)
    except Exception as e:
        print(get_linenumber(), "Something is wrong with the measure, error: ", e)
        return None
    return df
