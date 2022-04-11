# pythonLogProject
spark 를 이용한 pm2 '상태변경' 관련 log 분석

<br>

#### spark session을 이용하여 .log 확장자를 가진 모든 파일들을 읽어 'LOGS' templateView 생성

##### 조회시 value열에 텍스트가 행별로 들어가져있음

```python
spark.read.text('*.log').createOrReplaceTempView("LOGS")
```

<br>

#### value 값이 '상태 변경'을 포함하는 데이터 프레임 생성

```python
df = spark.sql("SELECT * FROM LOGS WHERE value like '%상태 변경%=>%'")
```

<br>

#### 가져온 데이터 전체 추출

```python
df.rdd.collect()
```

<br>

#### 추출한 데이터를 'CHANGE_STAT' templateView 로 재구성
|change_date|change_time|change_user|order_item_code|before_stat|after_stat|
|:---:|:---:|:---:|:---:|:---:|:---:|
|type:String|type:String|type:String|type:String|type:String|type:String|
