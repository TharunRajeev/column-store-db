def createTests16And17(dataTable, dataSize):
    # 1 / 1000 tuples should qualify on average. This is so that most time is spent on scans & not fetches or prints
    offset = np.max([1, int(dataSize / 5000)])
    query_starts = np.random.randint(0, (dataSize / 8), size=(100))
    output_file16, exp_output_file16 = data_gen_utils.openFileHandles(
        16, TEST_DIR=TEST_BASE_DIR
    )
    output_file17, exp_output_file17 = data_gen_utils.openFileHandles(
        17, TEST_DIR=TEST_BASE_DIR
    )
    output_file16.write("--\n")
    output_file16.write("-- Control timing for without batching\n")
    output_file16.write("-- Queries for 16 and 17 are identical.\n")
    output_file16.write("-- Query in SQL:\n")
    output_file16.write("-- 100 Queries of the type:\n")
    output_file16.write(
        "-- SELECT col3 FROM tbl3_batch WHERE col2 >= _ AND col2 < _;\n"
    )
    output_file16.write("--\n")
    output_file17.write("--\n")
    output_file17.write("-- Same queries with batching\n")
    output_file17.write("-- Queries for 16 and 17 are identical.\n")
    output_file17.write("--\n")
    output_file17.write("batch_queries()\n")
    for i in range(100):
        output_file16.write(
            "s{}=select(db1.tbl3_batch.col2,{},{})\n".format(
                i, query_starts[i], query_starts[i] + offset
            )
        )
        output_file17.write(
            "s{}=select(db1.tbl3_batch.col2,{},{})\n".format(
                i, query_starts[i], query_starts[i] + offset
            )
        )
    output_file17.write("batch_execute()\n")
    for i in range(100):
        output_file16.write("f{}=fetch(db1.tbl3_batch.col3,s{})\n".format(i, i))
        output_file17.write("f{}=fetch(db1.tbl3_batch.col3,s{})\n".format(i, i))
    for i in range(100):
        output_file16.write("print(f{})\n".format(i))
        output_file17.write("print(f{})\n".format(i))
    # generate expected results
    for i in range(100):
        dfSelectMask = (dataTable["col2"] >= query_starts[i]) & (
            (dataTable["col2"] < (query_starts[i] + offset))
        )
        output = dataTable[dfSelectMask]["col3"]
        exp_output_file16.write(data_gen_utils.outputPrint(output))
        exp_output_file16.write("\n\n")
        exp_output_file17.write(data_gen_utils.outputPrint(output))
        exp_output_file17.write("\n\n")
    data_gen_utils.closeFileHandles(output_file16, exp_output_file16)
    data_gen_utils.closeFileHandles(output_file17, exp_output_file17)
    return query_starts


def createTests18And19(dataTable, dataSize, query_starts):
    # 1 / 1000 tuples should qualify on average. This is so that most time is spent on scans & not fetches or prints
    offset = np.max([1, int(dataSize / 5000)])
    output_file18, exp_output_file18 = data_gen_utils.openFileHandles(
        18, TEST_DIR=TEST_BASE_DIR
    )
    output_file19, exp_output_file19 = data_gen_utils.openFileHandles(
        19, TEST_DIR=TEST_BASE_DIR
    )
    output_file18.write("--\n")
    output_file18.write(
        "-- Queries for 18 and 19 are single-core versions of Queries for 16 and 17.\n"
    )
    output_file18.write("-- Query in SQL:\n")
    output_file18.write("-- 100 Queries of the type:\n")
    output_file18.write(
        "-- SELECT col3 FROM tbl3_batch WHERE col2 >= _ AND col2 < _;\n"
    )
    output_file18.write("--\n")
    output_file19.write("--\n")
    output_file19.write("-- Same queries with single-core execution\n")
    output_file19.write(
        "-- Queries for 18 and 19 are single-core versions of Queries for 16 and 17.\n"
    )
    output_file19.write("--\n")
    output_file18.write("single_core()\n")
    output_file19.write("single_core()\n")
    output_file19.write("batch_queries()\n")
    for i in range(100):
        output_file18.write(
            "s{}=select(db1.tbl3_batch.col2,{},{})\n".format(
                i, query_starts[i], query_starts[i] + offset
            )
        )
        output_file19.write(
            "s{}=select(db1.tbl3_batch.col2,{},{})\n".format(
                i, query_starts[i], query_starts[i] + offset
            )
        )
    output_file19.write("batch_execute()\n")
    for i in range(100):
        output_file18.write("f{}=fetch(db1.tbl3_batch.col3,s{})\n".format(i, i))
        output_file19.write("f{}=fetch(db1.tbl3_batch.col3,s{})\n".format(i, i))
    output_file19.write("single_core_execute()\n")
    output_file18.write("single_core_execute()\n")
    for i in range(100):
        output_file18.write("print(f{})\n".format(i))
        output_file19.write("print(f{})\n".format(i))
    for i in range(100):
        dfSelectMask = (dataTable["col2"] >= query_starts[i]) & (
            (dataTable["col2"] < (query_starts[i] + offset))
        )
        output = dataTable[dfSelectMask]["col3"]
        exp_output_file18.write(data_gen_utils.outputPrint(output))
        exp_output_file18.write("\n\n")
        exp_output_file19.write(data_gen_utils.outputPrint(output))
        exp_output_file19.write("\n\n")
    data_gen_utils.closeFileHandles(output_file18, exp_output_file18)
    data_gen_utils.closeFileHandles(output_file19, exp_output_file19)
