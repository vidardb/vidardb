include ../../make_config.mk

.PHONY: clean libvidardb e2e-test

all: simple_row_test simple_column_test range_query_row_test range_query_column_test adaptive_table_factory_test

simple_row_test: libvidardb simple_row_test.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../../libvidardb.a -I../../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

simple_column_test: libvidardb simple_column_test.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../../libvidardb.a -I../../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

range_query_row_test: libvidardb range_query_row_test.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../../libvidardb.a -I../../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

range_query_column_test: libvidardb range_query_column_test.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../../libvidardb.a -I../../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

adaptive_table_factory_test: libvidardb adaptive_table_factory_test.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../../libvidardb.a -I../../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf ./simple_row_test ./simple_column_test ./range_query_row_test ./range_query_column_test ./adaptive_table_factory_test

libvidardb:
	cd ../.. && $(MAKE) static_lib

e2e-test: all
	sh run-e2e-tests.sh