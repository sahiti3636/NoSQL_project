import sys
C = sys.argv[1] + '/contracts'
q = lambda f: "'" + open(f'{C}/{f}').read().rstrip('\n').replace('\\', '\\\\').replace("'", "\\'") + "'"
ENV, REQ, FB = q('envelope.regex'), q('request.regex'), q('request_fallback.regex')
months = ','.join(l.replace('\t', ':') for l in open(f'{C}/months.tsv').read().split('\n')[1:] if l)
print(f"""SET hive.query.results.cache.enabled=false;
DROP TABLE IF EXISTS raw_sample;
CREATE EXTERNAL TABLE raw_sample (line STRING)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
  WITH SERDEPROPERTIES ('input.regex' = '([\\\\s\\\\S]*)')
  STORED AS TEXTFILE LOCATION 'file:///input/samples/input';
INSERT OVERWRITE LOCAL DIRECTORY '/runs/regex_test_hive'
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\'
SELECT CASE WHEN ok THEN 'OK' ELSE 'MALFORMED' END,
       if(ok, regexp_extract(line, {ENV}, 1), ''),
       if(ok, concat(regexp_extract(line, {ENV}, 6), '-', str_to_map('{months}', ',', ':')[regexp_extract(line, {ENV}, 5)], '-', regexp_extract(line, {ENV}, 4)), ''),
       if(ok, cast(cast(regexp_extract(line, {ENV}, 7) AS INT) AS STRING), ''),
       if(ok, if(req RLIKE {REQ}, regexp_extract(req, {REQ}, 1), ''), ''),
       if(ok, if(req RLIKE {REQ}, regexp_extract(req, {REQ}, 2), regexp_extract(req, {FB}, 1)), ''),
       if(ok, if(req RLIKE {REQ}, nvl(regexp_extract(req, {REQ}, 3), ''), ''), ''),
       if(ok, regexp_extract(line, {ENV}, 12), ''),
       if(ok, if(regexp_extract(line, {ENV}, 13) = '-', '0', regexp_extract(line, {ENV}, 13)), '')
FROM (SELECT line,
             line RLIKE {ENV} AND str_to_map('{months}', ',', ':')[regexp_extract(line, {ENV}, 5)] IS NOT NULL AS ok,
             regexp_extract(line, {ENV}, 11) AS req
      FROM raw_sample) t;
SELECT count(*) AS total_lines FROM raw_sample;
""")
