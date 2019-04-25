import boto3
import datetime


# 获取DynamoDB服务
client = boto3.client('dynamodb')


# 创建任务
def task_add(uid, description=None, dueat=None, category=None):
    try:
        cur = datetime.datetime.now()
        cur_lst = [
            cur.year, cur.month, cur.day,
            cur.hour, cur.minute, cur.second,
            cur.microsecond
        ]
        cur_lst = [str(i) for i in cur_lst]
        tid = ''.join(cur_lst)
        created = cur_lst[0] + cur_lst[1] + cur_lst[2]
        item = {
            'uid': {'S': uid},
            'tid': {'N': tid},
            'created': {'N': created}
        }
        if dueat:
            item['due'] = {'N': dueat}
        if description:
            item['description'] = {'S': description}
        if category:
            item['category'] = {'S': category}
        condition_expression = 'attribute_not_exists(uid) and attribute_not_exists(tid)'
        client.put_item(TableName='todo-task', Item=item,
                        ConditionExpression=condition_expression)
        print("任务创建成功 任务id：" + tid)
    except Exception as e:
        print(str(e))


# 条件主键查询
def search_task(uid, overdue=False, due=False, withoutdue=False, futuredue=False,
                category=None):
    yyyymmdd = datetime.datetime.now().strftime('%Y%m%d')
    key_condition_expresssion = 'uid = :uid'
    expression_attribute_values = {':uid': {'S': uid}}
    filter_expression = ''

    # 先对用户名进行查询，判断是否存在
    flag = client.query(TableName='todo-task',
                        KeyConditionExpression=key_condition_expresssion,
                        ExpressionAttributeValues=expression_attribute_values)

    # 条件主键查询
    if overdue:
        filter_expression = 'due < :yyyymmdd'
        expression_attribute_values[':yyyymmdd'] = {'N': yyyymmdd}
    elif due:
        filter_expression = 'due = :yyyymmdd'
        expression_attribute_values[':yyyymmdd'] = {'N': yyyymmdd}
    elif withoutdue:
        filter_expression = 'attribute_not_exists(due)'
    elif futuredue:
        filter_expression = 'due > :yyyymmdd'
        expression_attribute_values[':yyyymmdd'] = {'N': yyyymmdd}
    if category:
        if filter_expression == '':
            filter_expression = 'category = :category'
            expression_attribute_values[':category'] = {'S': category}
        else:
            filter_expression += ' AND category = :category'
            expression_attribute_values[':category'] = {'S': category}
    if filter_expression != '':
        response = client.query(TableName='todo-task',
                                FilterExpression=filter_expression,
                                KeyConditionExpression=key_condition_expresssion,
                                ExpressionAttributeValues=expression_attribute_values)
    else:
        response = client.query(TableName='todo-task',
                                KeyConditionExpression=key_condition_expresssion,
                                ExpressionAttributeValues=expression_attribute_values)
    flag = flag.get('Items')
    items = response.get('Items')
    if flag == []:
        print("用户'{0}'不存在".format(uid))
    elif items == []:
        print("无满足条件任务")
    else:
        results = []
        for item in items:
            result = {}
            result['用户名称'] = item.get('uid').get('S')
            result['任务编号'] = item.get('tid').get('N')
            result['创建时间'] = item.get('created').get('N')
            if 'due' in item:
                result['完成时间'] = item.get('due').get('N')
            if 'category' in item:
                result['类别'] = item.get('category').get('S')
            if 'description' in item:
                result['任务描述'] = item.get('description').get('S')
            results.append(result)
        print(results)


# 单一主键查询
def single_search(uid, tid):
    key = {
        'uid': {'S': uid},
        'tid': {'N': tid}
    }
    response = client.get_item(TableName='todo-task', Key=key)
    if 'Item' not in response:
        print("无该任务存在")
    else:
        item = response.get('Item')
        result = {'创建时间': item.get('created').get('N')}
        if 'due' in item:
            result['完成时间'] = item.get('due').get('N')
        if 'category' in item:
            result['类别'] = item.get('category').get('S')
        if 'description' in item:
            result['任务描述'] = item.get('description').get('S')
        print(result)


# 条件二级索引查询
def secondary_key_search_task(category, overdue=False, due=False, withoutdue=False, futuredue=False):
    yyyymmdd = datetime.datetime.now().strftime('%Y%m%d')
    key_condition_expresssion = 'category = :category'
    expression_attribute_values = {':category': {'S': category}}
    filter_expression = ''

    # 先对类别进行查询，判断是否存在
    flag = client.query(TableName='todo-task', IndexName='category-index',
                        KeyConditionExpression=key_condition_expresssion,
                        ExpressionAttributeValues=expression_attribute_values)

    # 条件主键查询
    if overdue:
        filter_expression = 'due < :yyyymmdd'
        expression_attribute_values[':yyyymmdd'] = {'N': yyyymmdd}
    elif due:
        filter_expression = 'due = :yyyymmdd'
        expression_attribute_values[':yyyymmdd'] = {'N': yyyymmdd}
    elif withoutdue:
        filter_expression = 'attribute_not_exists(due)'
    elif futuredue:
        filter_expression = 'due > :yyyymmdd'
        expression_attribute_values[':yyyymmdd'] = {'N': yyyymmdd}
    if filter_expression != '':
        response = client.query(TableName='todo-task', IndexName='category-index',
                                FilterExpression=filter_expression,
                                KeyConditionExpression=key_condition_expresssion,
                                ExpressionAttributeValues=expression_attribute_values)
    else:
        response = client.query(TableName='todo-task', IndexName='category-index',
                                KeyConditionExpression=key_condition_expresssion,
                                ExpressionAttributeValues=expression_attribute_values)
    flag = flag.get('Items')
    items = response.get('Items')
    if flag == []:
        print("类别'{0}'不存在".format(category))
    elif items == []:
        print("无满足条件任务")
    else:
        results = []
        for item in items:
            result = {}
            result['类别'] = item.get('category').get('S')
            result['用户名称'] = item.get('uid').get('S')
            result['任务编号'] = item.get('tid').get('N')
            result['创建时间'] = item.get('created').get('N')
            if 'due' in item:
                result['完成时间'] = item.get('due').get('N')
            if 'description' in item:
                result['任务描述'] = item.get('description').get('S')
            results.append(result)
        print(results)


# 遍历任务
def scan_task(limit=7, next_uid=None, next_tid=None):
    # 从某一位置开始遍历
    if next_uid and next_tid:
        exclusive_start_key = {
            'uid': {'S': next_uid},
            'tid': {'N': next_tid}
        }
        response = client.scan(TableName='todo-task', Limit=limit,
                               ExclusiveStartKey=exclusive_start_key)

    # 正常遍历
    else:
        response = client.scan(TableName='todo-task', Limit=limit)

    # 输出结果
    items = response.get('Items')
    if items == []:
        print("无任务")
    else:
        results = []
        for item in items:
            result = {}
            result['用户名称'] = item.get('uid').get('S')
            result['任务编号'] = item.get('tid').get('N')
            result['创建时间'] = item.get('created').get('N')
            if 'due' in item:
                result['完成时间'] = item.get('due').get('N')
            if 'category' in item:
                result['类别'] = item.get('category').get('S')
            if 'description' in item:
                result['任务描述'] = item.get('description').get('S')
            results.append(result)
        print(results)
        print('')
        if 'LastEvaluatedKey' not in response:
            print("已遍历完全部任务")
        else:
            last_evaluate_key = response.get('LastEvaluatedKey')
            uid = last_evaluate_key.get('uid').get('S')
            tid = last_evaluate_key.get('tid').get('N')
            print("当前遍历到 uid: {0}, tid: {1}".format(uid, tid))


# 删除任务
def delete_task(uid, tid):
    key = {
        'uid': {'S': uid},
        'tid': {'N': tid}
    }
    key_condition_expresssion = 'uid = :uid'
    expression_attribute_values = {':uid': {'S': uid}}
    response = client.query(TableName='todo-task',
                            KeyConditionExpression=key_condition_expresssion,
                            ExpressionAttributeValues=expression_attribute_values)
    items = response.get('Items')
    if items:
        tid_lst = [item.get('tid').get('N') for item in items]
        if tid in tid_lst:
            client.delete_item(TableName='todo-task', Key=key)
            print("任务已被删除")
        else:
            print("用户：'{0}'不存在任务：'{1}'".format(uid, tid))
    else:
        print("用户：'{0}'不存在".format(uid))


# 修改任务
def update_task(uid, tid, category=None, description=None, due=None):
    key = {
        'uid': {'S': uid},
        'tid': {'N': tid}
    }
    expression_attribute_values = {}
    update_expression = ''
    if category:
        update_expression = 'SET category = :category'
        expression_attribute_values[':category'] = {'S': category}
    if description:
        if update_expression == '':
            update_expression = 'SET description = :description'
        else:
            update_expression += ', description = :description'
        expression_attribute_values[':description'] = {'S': description}
    if due:
        if update_expression == '':
            update_expression = 'SET due = :due'
        else:
            update_expression += ', due = :due'
        expression_attribute_values[':due'] = {'N': due}
    if update_expression != '':
        client.update_item(TableName='todo-task', Key=key,
                           UpdateExpression=update_expression,
                           ExpressionAttributeValues=expression_attribute_values)
        print("更新完成")
    else:
        print("请输入需要更新的属性")


if __name__ == '__main__':

    # 创建任务表
    attribute_definitions = [
        {
            'AttributeName': 'uid', 'AttributeType': 'S'
        },
        {
            'AttributeName': 'tid', 'AttributeType': 'N'
        },
        {
            'AttributeName': 'category', 'AttributeType': 'S'
        }
    ]
    table_name = 'todo-task'
    key_schema = [
        {
            'AttributeName': 'uid', 'KeyType': 'HASH'
        },
        {
            'AttributeName': 'tid', 'KeyType': 'RANGE'
        }
    ]
    global_secondary_indexes = [
        {
            'IndexName': 'category-index',
            'KeySchema': [
                {'AttributeName': 'category', 'KeyType': 'HASH'},
                {'AttributeName': 'tid', 'KeyType': 'RANGE'}
            ],
            'Projection': {'ProjectionType': 'ALL'},
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5
            }
        }
    ]
    provisioned_throughput = {
        'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5
    }
    response = client.create_table(
        AttributeDefinitions=attribute_definitions,
        TableName=table_name, KeySchema=key_schema,
        GlobalSecondaryIndexes=global_secondary_indexes,
        ProvisionedThroughput=provisioned_throughput)

    # 添加任务（表创建完成之后再添加任务）
    """task_add('xyj', description="just a test",
             category='one', dueat='20190310')
    task_add('hy', description="just a test",
             category='one', dueat='20190202')
    task_add('xyj', description="just a test", category='one')
    task_add('xyj', description="just a test",
             category="two", dueat='20190328')
    task_add('hy', description="just a test",
             category='one', dueat='20190110')
    task_add('xyj', description="just a test",
             category="one", dueat='20190728')
    task_add('xyj', description="just a test",
             category="two", dueat='20190828')
    task_add('hy', description="just a test",
             category='two', dueat='20190521')
    task_add('hy', description="just a test", category='two')
    task_add('hy', description="just a test", category='one')
    task_add('hy', description="just a test",
             category='two', dueat='20190424')
    task_add('hy', dueat='20190627')"""

    # 查询任务
    # search_task('hy', category='one')
    # single_search('xyj', '2019424221933622647')
    # secondary_key_search_task('one', withoutdue=True)

    # 遍历查找任务
    # scan_task()
    # scan_task(next_uid='hy', next_tid='2019425135227271930')

    # 删除任务
    # delete_task('xyj', '2019425135227202088')

    # 更新任务
    """update_task('xyj', '2019425135227131830', category='two',
                description='just a test', due='20191206')"""
