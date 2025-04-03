#!/usr/bin/python
import sys, string
from random import choice
import random
from string import ascii_lowercase
from scipy.stats import beta, uniform
import numpy as np
import struct
import pandas as pd
import os
import data_gen_utils


def create_tests_for_batching_impact_multicore(
    dataTable, dataSize, ctrl_tst_num, batched_tst_num, batch_size, dest_dir
):
    """
    This function generates two tests, one with batching and one without.

    """
    # 1 / 1000 tuples should qualify on average.
    # This is so that most time is spent on scans & not fetches or prints
    offset = np.max([1, int(dataSize / 5000)])
    query_starts = np.random.randint(0, (dataSize / 8), size=(batch_size))
    output_file_ctrl, exp_output_file_ctrl = data_gen_utils.openFileHandles(
        ctrl_tst_num, TEST_DIR=dest_dir
    )
    output_file_batched, exp_output_file_batched = data_gen_utils.openFileHandles(
        batched_tst_num, TEST_DIR=dest_dir
    )
    output_file_ctrl.write("--\n")
    output_file_ctrl.write("-- Control timing for without batching\n")
    output_file_ctrl.write(
        f"-- Queries for {ctrl_tst_num} and {batched_tst_num} are identical.\n"
    )
    output_file_ctrl.write("-- Query in SQL:\n")
    output_file_ctrl.write(f"-- {batch_size} Queries of the type:\n")
    output_file_ctrl.write(
        "-- SELECT col3 FROM tbl3_batch WHERE col2 >= _ AND col2 < _;\n"
    )
    output_file_ctrl.write("--\n")
    output_file_batched.write("--\n")
    output_file_batched.write("-- Same queries with batching\n")
    output_file_batched.write(
        f"-- Queries for {ctrl_tst_num} and {batched_tst_num} are identical.\n"
    )
    output_file_batched.write("--\n")
    output_file_batched.write("batch_queries()\n")
    for i in range(batch_size):
        output_file_ctrl.write(
            "s{}=select(db1.tbl3_batch.col2,{},{})\n".format(
                i, query_starts[i], query_starts[i] + offset
            )
        )
        output_file_batched.write(
            "s{}=select(db1.tbl3_batch.col2,{},{})\n".format(
                i, query_starts[i], query_starts[i] + offset
            )
        )
    output_file_batched.write("batch_execute()\n")
    for i in range(batch_size):
        output_file_ctrl.write("f{}=fetch(db1.tbl3_batch.col3,s{})\n".format(i, i))
        output_file_batched.write("f{}=fetch(db1.tbl3_batch.col3,s{})\n".format(i, i))
    for i in range(batch_size):
        output_file_ctrl.write("print(f{})\n".format(i))
        output_file_batched.write("print(f{})\n".format(i))
    # generate expected results
    for i in range(batch_size):
        dfSelectMask = (dataTable["col2"] >= query_starts[i]) & (
            (dataTable["col2"] < (query_starts[i] + offset))
        )
        output = dataTable[dfSelectMask]["col3"]
        exp_output_file_ctrl.write(data_gen_utils.outputPrint(output))
        exp_output_file_ctrl.write("\n\n")
        exp_output_file_batched.write(data_gen_utils.outputPrint(output))
        exp_output_file_batched.write("\n\n")
    data_gen_utils.closeFileHandles(output_file_ctrl, exp_output_file_ctrl)
    data_gen_utils.closeFileHandles(output_file_batched, exp_output_file_batched)
    return query_starts


def create_tests_for_batching_impact_singlecore(
    dataTable,
    dataSize,
    query_starts,
    ctrl_tst_num,
    batched_tst_num,
    batch_size,
    dest_dir,
):
    # 1 / 1000 tuples should qualify on average.
    # This is so that most time is spent on scans & not fetches or prints
    offset = np.max([1, int(dataSize / 5000)])
    output_file_ctrl, exp_output_file_ctrl = data_gen_utils.openFileHandles(
        ctrl_tst_num, TEST_DIR=dest_dir
    )
    output_file_batched, exp_output_file_batched = data_gen_utils.openFileHandles(
        batched_tst_num, TEST_DIR=dest_dir
    )
    output_file_ctrl.write("--\n")
    output_file_ctrl.write(
        f"-- Queries for {ctrl_tst_num} and {batched_tst_num} are single-core"
        "for testing impact of batching on single-core"
    )
    output_file_ctrl.write("-- Query in SQL:\n")
    output_file_ctrl.write(f"-- {batch_size} Queries of the type:\n")
    output_file_ctrl.write(
        "-- SELECT col3 FROM tbl3_batch WHERE col2 >= _ AND col2 < _;\n"
    )
    output_file_ctrl.write("--\n")
    output_file_batched.write("--\n")
    output_file_batched.write("-- Same queries with single-core execution\n")
    output_file_batched.write(
        f"-- Queries for {ctrl_tst_num} and {batched_tst_num} are single-core"
    )
    output_file_batched.write("--\n")
    output_file_ctrl.write("single_core()\n")
    output_file_batched.write("single_core()\n")
    output_file_batched.write("batch_queries()\n")
    for i in range(batch_size):
        output_file_ctrl.write(
            "s{}=select(db1.tbl3_batch.col2,{},{})\n".format(
                i, query_starts[i], query_starts[i] + offset
            )
        )
        output_file_batched.write(
            "s{}=select(db1.tbl3_batch.col2,{},{})\n".format(
                i, query_starts[i], query_starts[i] + offset
            )
        )
    output_file_batched.write("batch_execute()\n")
    for i in range(batch_size):
        output_file_ctrl.write("f{}=fetch(db1.tbl3_batch.col3,s{})\n".format(i, i))
        output_file_batched.write("f{}=fetch(db1.tbl3_batch.col3,s{})\n".format(i, i))
    output_file_batched.write("single_core_execute()\n")
    output_file_ctrl.write("single_core_execute()\n")
    for i in range(batch_size):
        output_file_ctrl.write("print(f{})\n".format(i))
        output_file_batched.write("print(f{})\n".format(i))
    for i in range(batch_size):
        dfSelectMask = (dataTable["col2"] >= query_starts[i]) & (
            (dataTable["col2"] < (query_starts[i] + offset))
        )
        output = dataTable[dfSelectMask]["col3"]
        exp_output_file_ctrl.write(data_gen_utils.outputPrint(output))
        exp_output_file_ctrl.write("\n\n")
        exp_output_file_batched.write(data_gen_utils.outputPrint(output))
        exp_output_file_batched.write("\n\n")
    data_gen_utils.closeFileHandles(output_file_ctrl, exp_output_file_ctrl)
    data_gen_utils.closeFileHandles(output_file_batched, exp_output_file_batched)
