cql_row_processor
==========

Python script that process all rows in a column family one by one, allowing to apply custom logic on it.

### Usage

To see how to use it, use --help option:

```
./process_cf.py --help
```

When using a module, use --help option on the module:

```
./process_cf.py -m module.csv_exporter --help
```

### Example usage

To export cf identification.entitylookup, connecting though localhost, using 3 parallel processes, to a csv like output:
```
./process_cf.py -w 3 -H localhost -ks identification -cf entitylookup -q True -s 1 -l 10 -m module.csv_exporter -d\|
msisdn|5e543256c480ac577d30f76f9120eb74|4ae534d8-5ac7-4c85-8a6d-8b7d1a9260b6
identifier14_email|0299385BD06D69DC9C86E2A4F460D63C|1d559a8f-cb74-4970-96ca-8e5f7c05a208
identifier14_email|036CFB0B3483CF2C8FEAC80226444043|66eb55a7-f7e4-4fb5-93e1-c079c5f21e13
identifier14_email|037DECED4200713F2C6463E1D522481B|d9f1379c-2373-4608-ba96-abb5f7969d06
identifier14_email|0773C9E49F86F53B4F018FCE8DD6ABD6|69bc87c7-e235-4397-bb2e-12d3b1d43431
identifier14_email|07A53C9373E0C10E30F92D1989699871|a6c52502-e3ff-4ffb-8a4e-0c91b2cc7bae
identifier14_email|086A2130D27C6BF6E5B84059A52A4DF0|7d1d1612-e183-4b4b-b424-33ea4956b74e
identifier14_email|0B04EE9DF3E8B107D516B387FEBE9F4C|5e462011-cd41-4c5f-ae5a-da7c3abb93a1
identifier14_email|0E22937BCA7B98C5ED0B648E847AB6BD|2fb1f7bd-8294-49e4-8623-88490250de29
identifier14_email|12D85F08A1CCEB844F966BCB1DC45B36|e563ef71-1b4f-488b-9c0c-231428c4e57b
```


