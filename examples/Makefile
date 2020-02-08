include ../make_config.mk

.PHONY: clean libvidardb

all: simple_example column_families_example compact_files_example simple_column_example range_query_example range_query_column_example adaptive_table_example

simple_example: libvidardb simple_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../libvidardb.a -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

column_families_example: libvidardb column_families_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../libvidardb.a -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

compact_files_example: libvidardb compact_files_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../libvidardb.a -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

transaction_example: libvidardb transaction_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../libvidardb.a -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

simple_column_example: libvidardb simple_column_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../libvidardb.a -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

range_query_example: libvidardb range_query_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../libvidardb.a -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

range_query_column_example: libvidardb range_query_column_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../libvidardb.a -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

adaptive_table_example: libvidardb adaptive_table_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../libvidardb.a -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf ./simple_example ./column_families_example ./compact_files_example ./transaction_example ./simple_column_example ./range_query_example ./range_query_column_example ./adaptive_table_example

libvidardb:
	cd .. && $(MAKE) static_lib
