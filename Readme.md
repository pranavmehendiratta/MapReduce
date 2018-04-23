[Test1 - Sorting 1 file(s) with 1 mapper(s), 1 reducer(s) and default partitioner]
  Running $./sort_m1r1 /u/c/s/cs537-1/ta/tests/4a-new/inputs/wordlist.in
** 106.841 ms elapsed **
[Test1 Success]

[Test2 - Sorting 1 file(s) with 4 mapper(s), 1 reducer(s) and default partitioner]
  Running $./sort_m4r1 /u/c/s/cs537-1/ta/tests/4a-new/inputs/wordlist.in
** 101.972 ms elapsed **
[Test2 Success]

[Test3 - Sorting 1 file(s) with 1 mapper(s), 4 reducer(s) and default partitioner]
  Running $./sort_m1r4 /u/c/s/cs537-1/ta/tests/4a-new/inputs/wordlist.in
** 217.589 ms elapsed **
[Test3 Success]

[Test4 - Sorting 1 file(s) with 4 mapper(s), 4 reducer(s) and default partitioner]
  Running $./sort_m4r4 /u/c/s/cs537-1/ta/tests/4a-new/inputs/wordlist.in
** 228.235 ms elapsed **
[Test4 Success]

[Test5 - Sorting 4 file(s) with 1 mapper(s), 1 reducer(s) and default partitioner]
  Running $./sort_m1r1 /u/c/s/cs537-1/ta/tests/4a-new/inputs/wordlist.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/wordlist.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/wordlist.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/wordlist.in
** 289.410 ms elapsed **
[Test5 Success]

[Test6 - Sorting 4 file(s) with 4 mapper(s), 4 reducer(s) and default partitioner]
  Running $./sort_m4r4 /u/c/s/cs537-1/ta/tests/4a-new/inputs/wordlist.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/wordlist.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/wordlist.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/wordlist.in
** 521.695 ms elapsed **
[Test6 Success]

[Test7 - Wordcounting 1 file(s) with 1 mapper(s) and 1 reducer(s) and default partitioner]
(1mb file)
  Running $./wordcount_m1r1 /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in
** 98.509 ms elapsed **
[Test7 Success]

[Test8 - Wordcounting 1 file(s) with 1 mapper(s) and 1 reducer(s) and default partitioner]
(100mb file)
  Running $./wordcount_m1r1 /u/c/s/cs537-1/ta/tests/4a-new/inputs/file100m.in
** 7159.284 ms elapsed **
[Test8 Success]

[Test9 - Wordcounting 1 file(s) with 1 mapper(s) and 4 reducer(s) and default partitioner]
(1mb file)
  Running $./wordcount_m1r4 /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in
** 186.574 ms elapsed **
[Test9 Success]

[Test10 - Wordcounting 1 file(s) with 1 mapper(s) and 20 reducer(s) and default partitioner]
(1mb file)
  Running $./wordcount_m1r20 /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in
** 717.739 ms elapsed **
[Test10 Success]

[Test11 - Wordcounting 20 file(s) with 1 mapper(s) and 1 reducer(s) and default partitioner]
(1mb file * 20)
  Running $./wordcount_m1r1 /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in
** 731.508 ms elapsed **
[Test11 Success]

[Test12 - Wordcounting 20 file(s) with 4 mapper(s) and 1 reducer(s) and default partitioner]
(1mb file * 20)
  Running $./wordcount_m4r1 /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in
** 908.312 ms elapsed **
[Test12 Success]

[Test13 - Wordcounting 20 file(s) with 1 mapper(s) and 4 reducer(s) and default partitioner]
(1mb file * 20)
  Running $./wordcount_m1r4 /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in
** 825.582 ms elapsed **
[Test13 Success]

[Test14 - Wordcounting 20 file(s) with 20 mapper(s) and 1 reducer(s) and default partitioner]
(1mb file * 20)
  Running $./wordcount_m20r1 /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in
** 945.667 ms elapsed **
[Test14 Success]

[Test15 - Wordcounting 20 file(s) with 1 mapper(s) and 20 reducer(s) and default partitioner]
(1mb file * 20)
  Running $./wordcount_m1r20 /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in
** 1326.501 ms elapsed **
[Test15 Success]

[Test16 - Wordcounting 5 file(s) with 5 mapper(s) and 1 reducer(s) and default partitioner]
(100mb file *1 , 1mb file * 4)
  Running $./wordcount_m5r1 /u/c/s/cs537-1/ta/tests/4a-new/inputs/file100m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in
** 7517.659 ms elapsed **
[Test16 Success]

[Test17 - Wordcounting 1 file(s) with 1 mapper(s) and 10 reducer(s) and custom partitioner]
(1mb file)
  Running $./wordcount_cp_m1r10 /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in
** 379.310 ms elapsed **
[Test17 Success]

[Test18 - Wordcounting 1 file(s) with 1 mapper(s) and 50 reducer(s) and custom partitioner]
(1mb file)
  Running $./wordcount_cp_m1r50 /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in
** 1733.665 ms elapsed **
[Test18 Success]

[Test19 - Wordcounting iterates 2 times 10 file(s) with 1 mapper(s) and 1 reducer(s) and default partitioner]
(1mb file * 10)
  Running $./wordcount_multi_m1r1 /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in
** 807.412 ms elapsed for 0th iteration **
** 1140.632 ms elapsed for 1th iteration **
** total 1948.044 ms elapsed **
[Test19 Success]

[Test20 - Wordcounting iterates 10 times 10 file(s) with 10 mapper(s) and 1 reducer(s) and default partitioner]
(1mb file * 10)
  Running $./wordcount_multi_m10r1 /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in
** 986.836 ms elapsed for 0th iteration **
** 1188.689 ms elapsed for 1th iteration **
** 1187.495 ms elapsed for 2th iteration **
** 1176.347 ms elapsed for 3th iteration **
** 1227.837 ms elapsed for 4th iteration **
** 1264.498 ms elapsed for 5th iteration **
** 1217.307 ms elapsed for 6th iteration **
** 1231.200 ms elapsed for 7th iteration **
** 1289.803 ms elapsed for 8th iteration **
** 1264.099 ms elapsed for 9th iteration **
** total 12034.111 ms elapsed **
[Test20 Success]

[Test21 - Wordcounting iterates 10 times 10 file(s) with 10 mapper(s) and 4 reducer(s) and default partitioner]
(1mb file * 10)
  Running $./wordcount_multi_m10r4 /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in /u/c/s/cs537-1/ta/tests/4a-new/inputs/file1m.in
** 1053.295 ms elapsed for 0th iteration **
** 1262.143 ms elapsed for 1th iteration **
** 1306.305 ms elapsed for 2th iteration **
** 1327.365 ms elapsed for 3th iteration **
** 1382.218 ms elapsed for 4th iteration **
** 1324.959 ms elapsed for 5th iteration **
** 1306.696 ms elapsed for 6th iteration **
** 1353.082 ms elapsed for 7th iteration **
** 1374.782 ms elapsed for 8th iteration **
** 1290.281 ms elapsed for 9th iteration **
** total 12981.126 ms elapsed **
[Test21 Success]


Test Result (21/21)
