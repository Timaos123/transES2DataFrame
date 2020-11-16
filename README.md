# 建立ES对象


```python
from elasticsearch import Elasticsearch, helpers
```


```python
es_host="xxx"
es_user="xxx"
es_password="xxx"
es = Elasticsearch([es_host], http_auth=(es_user, es_password))
```

# 构造查询语句


```python
qbody={
      xxx
    }

scroll_size = 1000

query = es.search(index="xxx", body=qbody)
res = query['hits']['hits']
```

# 递归获取DataFrame

当出现嵌套情况时，使用“.”将上下级包含关系进行衔接，并删除原上级文档所对应的列


```python
import pandas as pd

def buildDtFromES(res):
    resDf=pd.DataFrame(res)
    columnList=resDf.columns
    for columnItem in columnList:
        tmpValList=resDf[columnItem].values.tolist()
        tmpTypeCheckList=[type(tmpValItem)==dict for tmpValItem in tmpValList]
        if len(tmpTypeCheckList)==sum(tmpTypeCheckList):
            tmpDf=buildDtFromES(tmpValList)
            tmpColumnList=tmpDf.columns
            renameColumnDict=dict((tmpColumnItem,columnItem+"."+tmpColumnItem) for tmpColumnItem in tmpColumnList)
            tmpDf.rename(renameColumnDict,axis=1,inplace=True)
            resDf=pd.concat([resDf,tmpDf],axis=1)
            resDf.drop(columnItem,axis=1,inplace=True)
    return resDf.copy(deep=True)
```

# 样例展示


```python
buildDtFromES(res).head().iloc[:,10:11]
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>_source.RecruitStaffTypeID</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>


