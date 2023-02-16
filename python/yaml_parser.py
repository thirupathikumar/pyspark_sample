import yaml
import io
'''
---
doe: "a deer, a female deer"
ray: "a drop of golden sun"
pi: 3.14159
xmas: true
french-hens: 3
calling-birds:
   - huey
   - dewey
   - louie
   - fred
xmas-fifth-day:
   calling-birds: four
   french-hens: 3
   golden-rings: 5
   partridges:
     count: 1
     location: "a pear tree"
   turtle-doves: two

what is yaml file 
1.The file starts with three dashes. These dashes indicate the start of a new YAML document.
YAML supports multiple documents, and compliant parsers will recognize each set of dashes as the beginning of a new one

2.Doe is a key that points to a string value: a deer, a female deer. 
YAML supports more than just string values. The file starts with six key-value pairs. 
They have four different data types. 
Doe and ray are strings. 
Pi is a floating-point number. 
Xmas is a boolean. 
French-hens is an integer. 
You can enclose strings in single or double-quotes or no quotes at all. 
YAML recognizes unquoted numerals as integers or floating point. 
The seventh item is an array. Calling-birds has four elements, each denoted by an opening dash. 
I indented the elements in calling-birds with two spaces. 
Indentation is how YAML denotes nesting. 
The number of spaces can vary from file to file, but tabs are not allowed. 
We'll look at how indentation works below. 
Finally, we see xmas-fifth-day, which has five more elements inside it, each of them indented. 
We can view xmas-fifth-day as a dictionary that contains two string, two integers, and another dictionary.
'''

table_mapping:
 db.table1:
  col1: int
  col2: string
 db.table2:
  col3: int
  col4: string
spark_hive_datatype_mapping:
 ByteType: tinyint
 ShortType: smallint
 IntegerType: integer
 LongType: long
 StringType: string
 FloatType: float
 DoubleType: double
 DecimalType: decimal
 DateTimeType: datetime
 TimestampType: timestamp
 
 
 dct = dict()
with open("D:\Thiruppathi\src\mypublicgit\pyspark_sample\python\input.yaml") as f:
    config=yaml.load(f,Loader=yaml.FullLoader)
    #print(config['db.table1']['col1'])
    #print(config['db.table1']['col2'])
    for itm in config['table_mapping']:
        #print(itm)
        for k in config['table_mapping'][itm]:
            #print(k)
            #print(config['table_mapping'][itm][k])
            #dct[str(config['table_mapping'][itm])] = str(config['table_mapping'][itm][k])
            key=itm+"."+k
            val=config['table_mapping'][itm][k]
            #print(key)
            #print(val)
            dct[key] = val
            
    for spitm in config['spark_hive_datatype_mapping']:
        #print(spitm)
        #print(config['spark_hive_datatype_mapping'][spitm])
        dct[spitm] = config['spark_hive_datatype_mapping'][spitm]
            
        

#print(dct)
for k,v in dct.items():
    print(k, ":", v)