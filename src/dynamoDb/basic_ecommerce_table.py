'''
This is the application code to build an eCommerce dynamo db table called EcommerceCartsAndOrders. For fast retrieval, multiple
tables/events are modelled in single nosql schema, to reduce joins and increase spoeed

Primary Key: user_id (String)
Sort_Key: item_key (String)    - can be CART#<product_id> or ORDER#<order_id> depending on event

Event Order: (each step shows the current state of dynamoDB after operation)

1. User adds Nike Shoes (prod_550) to cart

user_id,item_key,status,payload
usr_101,CART#prod_550,IN_CART,{"product": "Nike Shoes", "qty": 1, "unit_price": 4500, "city": "Hyderabad"}

2. User adds Earbuds (prod_880) to cart

user_id,item_key,status,payload
usr_101,CART#prod_550,IN_CART,{"product": "Nike Shoes", "qty": 1, "unit_price": 4500, "city": "Hyderabad"}
usr_101,CART#prod_880,IN_CART,{"product": "Earbuds", "qty": 1, "unit_price": 1999, "city": "Hyderabad"}

3. User updates Nike Shoes quantity from 1 to 2

user_id,item_key,status,payload
usr_101,CART#prod_550,IN_CART,{"product": "Nike Shoes", "qty": 2, "unit_price": 4500, "city": "Hyderabad"}
usr_101,CART#prod_880,IN_CART,{"product": "Earbuds", "qty": 1, "unit_price": 1999, "city": "Hyderabad"}

4. User deletes Earbuds (prod_880) from cart

user_id,item_key,status,payload
usr_101,CART#prod_550,IN_CART,{"product": "Nike Shoes", "qty": 2, "unit_price": 4500, "city": "Hyderabad"}

5. User checks out and pays for Nike Shoes (ord_991)

user_id,item_key,status,payload

usr_101,ORDER#ord_991,PURCHASED,{"order_id": "ord_991", "payment": "UPI", "order_date": "2015-12-21T17:42:34Z", 
"items": [{"product_id": "prod_550", "qty": 2, "unit_price": 4500}]}



Access patterns:

1) Get order for a user- query on user_id and item_key = ORDER#ord_991
2) Get all orders for a user- query on user_id and item_key like '%ORDER%'
3) Get current cart for a user - query on user_id and item_key like '%CART%' . Only 1 cart active at a time.


'''


'''
THis code randomly selects an event [add,update,delete, checkout] and executes it in DynamoDB. It also has guardrails for edge cases like:
1) delete event on empty cart
2) duplicate event (user+ product) addition

etc....

Order of running code:
1) To run only the program to only create table run 'python3 "/Users/sreejitchakraborty/Documents/personal projects/aws-learning/src/dynamoDb/basic_ecommerce_table.py" create_table_only'
2) To run event generation run 'python3 "/Users/sreejitchakraborty/Documents/personal projects/aws-learning/src/dynamoDb/basic_ecommerce_table.py"'

Run 1 then run 2


To start over - pl delete the table and re-run, otherwise there might be duplicates in add_event, since it depends on local variable.

'''

import boto3
import time
import faker
import random
import datetime
import uuid
import sys



def does_table_exist(client,table_name):
    # Note dynamo tables are region specific and 2 tables can share names in different regions.
    # this checks in the default region as specified in boto client

    full_table_list=[]

    resp=client.list_tables(Limit=100)
    last_=resp.get('LastEvaluatedTableName')
    full_table_list.extend(resp['TableNames'])


    while(last_ is not None):
        resp=client.list_tables(ExclusiveStartTableName=last_,Limit=100)

        if(len(resp['TableNames'])==0):
            print("No more tables found")
            break

        else:
            full_table_list.extend(resp['TableNames'])
            last_=resp['LastEvaluatedTableName']

    if(table_name not in full_table_list):
        print("Not found!!!")
        return False
    
    print("Found!!!")
    return True

def create_table(client,table_name):

    # remember only fields used in search like Partition Key, Sort Key, or Index Key should be put in AttributeDefinitions
    # any other column/field can be inserted directly


    '''
    
    For a simple primary key (partition key), you must provide exactly one element with a KeyType of HASH.

    For a composite primary key (partition key and sort key), you must provide exactly two elements, 
    in this order: The first element must have a KeyType of HASH, and the second element must have a KeyType of RANGE
    
    '''

    r=client.create_table(
        AttributeDefinitions= [
        {
            'AttributeName': 'user_id',
            'AttributeType': 'S',
        },
        {
            'AttributeName': 'item_key',
            'AttributeType': 'S',
        }],
         KeySchema=[
        {
            'AttributeName': 'user_id',
            'KeyType': 'HASH',
        },
        {
            'AttributeName': 'item_key',
            'KeyType': 'RANGE',
        }],
        TableName=table_name,
        BillingMode='PAY_PER_REQUEST',
        StreamSpecification={
            "StreamEnabled":True,
            "StreamViewType":"NEW_AND_OLD_IMAGES"
        }
        )

    # above operation is aysc, so we poll to see status

    while(True):

        response = client.describe_table(
            TableName=table_name
        )

        if(response['Table']['TableStatus']=='ACTIVE'):

            print("Table created....")
            break

        elif(response['Table']['TableStatus'] in ['INACCESSIBLE_ENCRYPTION_CREDENTIALS','REPLICATION_NOT_AUTHORIZED']):
            print("Table creation failed..")
            break

        else:
            print("creating....")
            time.sleep(5)


def generate_fake_event(client,table_name):

    all_users=['user1','user2','user3','user4','user5','user6','user7','user8','user9','user10']

    all_products=[{'name':'product1','unit_price':random.randint(1,1000)},
                  {'name':'product2','unit_price':random.randint(1,1000)},
                  {'name':'product3','unit_price':random.randint(1,1000)},
                  {'name':'product4','unit_price':random.randint(1,1000)},
                  {'name':'product5','unit_price':random.randint(1,1000)}]
    
    f=faker.Faker(locale='en_IN')
  

    def add_to_cart(client,table_name):

        print("ADD EVENT..")

        global in_cart
        
        avail_comb=[] #current available user_id and product_id in cart competetion

        for u in all_users:
            for p in all_products:

                key = f'CART#{p["name"]}'

                if (any( x['user_id'] == u and x['key'] == key for x in in_cart) ==False):   # no such combination in in_cart
                            avail_comb.append((u, p))

        if(len(avail_comb)==0):
            print("No remaining combinations to add....")
            return


        fake_tup= random.choice(avail_comb)
        fake_user= fake_tup[0]
        fake_product=fake_tup[1]


        key=f'CART#{fake_product["name"]}'
        status='IN_CART'
        fake_city=f.city()

        payload={
            'product':fake_product['name'],
            'qty':f'{random.randint(1,100)}',
            'unit_price':f"{fake_product['unit_price']}",
            'city':fake_city
        }


        client.put_item(
            TableName=table_name,
            Item={
                'user_id':{
                    'S':fake_user
                },
                'item_key':{
                    'S':key
                },
                'status':{
                    'S':status
                },
                'payload':{
                    'M':{
                        "product":{'S':payload['product']},
                        "qty":{'S':payload['qty']},
                        "unit_price":{'S':payload['unit_price']},
                        "city":{'S':payload['city']}
                        }
                }

            }
        )

        print(f"added item...... user_id: {fake_user} and key: {key}")

        in_cart.append( {"user_id":fake_user,"key":key,"status":status,"payload":payload})


    def update_in_cart(client,table_name):

        print("UPDATE EVENT..")

        global in_cart

        cart_items = [
        i for i in in_cart
        if i['key'].startswith('CART#')
        ]

        if(len(cart_items)==0):
            print("No items found to update..")
            return

        upd=random.choice(cart_items) # select random product to update

        # for this demo we only update quantity to a new quantity

        fake_user= upd['user_id']
        key=upd['key']
        status=upd['status']


        payload=upd['payload']

        payload.update({"qty":f'{random.randint(1,100)}'})


        client.put_item(
            TableName=table_name,
            Item={
                'user_id':{
                    'S':fake_user
                },
                'item_key':{
                    'S':key
                },
                'status':{
                    'S':status
                },
                'payload':{
                    'M':{
                        "product":{'S':payload['product']},
                        "qty":{'S':payload['qty']},
                        "unit_price":{'S':payload['unit_price']},
                        "city":{'S':payload['city']}
                        }
                }

            }
        )

        print(f"updated item...... user_id: {fake_user} and key: {key} to quantity {payload['qty']}")

        in_cart= [d for d in in_cart if not  ( (d.get('user_id')== fake_user) and (d.get('key')== key) ) ]

        in_cart.append( {"user_id":fake_user,"key":key,"status":status,"payload":payload})


    def delete_from_cart(client,table_name):

        print("DELETE EVENT..")

        global in_cart

        cart_items = [
    i for i in in_cart
    if i['key'].startswith('CART#')
    ]

        if(len(cart_items)==0):
            print("No items found to delete..")
            return


        dele=random.choice(cart_items) # select random product to delete
        fake_user= dele['user_id']
        key=dele['key']


        client.delete_item(
        Key={
            'user_id': {
                'S': fake_user,
            },
            'item_key': {
                'S': key,
            },
        },
        TableName=table_name,
        )

        print(f"deleted item...... user_id: {fake_user} and key: {key} ")

        in_cart= [d for d in in_cart  if not (
        d.get('user_id') == fake_user
        and d.get('key') == key) ]


    def checkout(client,table_name):

        print("CHECKOUT EVENT..")

        global in_cart

        user_carts = [
        i for i in in_cart
        if i['key'].startswith('CART#')
        ]

        if(len(user_carts)==0):
            print("No active carts found to checkout..")
            return
        

        checkout=random.choice(user_carts) # select random user cart to checkout

        fake_user= checkout['user_id']

        order_id=str(uuid.uuid4())
        key=f'ORDER#{order_id}'
        status='PURCHASED'
        order_date=datetime.datetime.now()
        order_date_format=order_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        payment=random.choice(['UPI','CASH','NETBANKING','CARD'])

        all_payload= [i['payload'] for i in in_cart if i.get('user_id')==fake_user and i['key'].startswith('CART#')]


        mod_payload=[]
        order_payload=[]

        for i in all_payload:
   
            np={
                'M':{
                    'product_id':{'S':i['product']},
                    'qty':{'S':str(i['qty'])},
                    'unit_price':{'S':str(i['unit_price'])},
                }
            }

            zp={
                'product_id':i['product'],
                'qty':str(i['qty']),
                'unit_price':str(i['unit_price']),
            }
        

            mod_payload.append(np)
            order_payload.append(zp)

        


        # insert the order 
        client.put_item(
            TableName=table_name,
            Item={
                'user_id':{
                    'S':fake_user
                },
                'item_key':{
                    'S':key
                },
                'status':{
                    'S':status
                },
                'payload':{
                    'M':{
                        "order_id":{'S':order_id},
                        "payment":{'S':payment},
                        "order_date":{'S':order_date_format},
                        "items":{'L':mod_payload}
                        }
                }

            }
        )

        # delete the carts

        for m in in_cart:

            if(m['user_id']==fake_user and m['key'].startswith('CART#')):

                client.delete_item(
                Key={
                    'user_id': {
                        'S': fake_user,
                    },
                    'item_key': {
                        'S': m['key'],
                    },
                },
                TableName=table_name,
                )

                print(f"Deleted key {m['key']} for {fake_user}")

        in_cart = [
            m for m in in_cart
            if not (m['user_id'] == fake_user and m['key'].startswith('CART#'))
            ]
        
       


        order_payload = {
        'order_id': order_id,
        'payment': payment,
        'order_date': order_date_format,
        'items': order_payload
    }


        in_cart.append( {"user_id":fake_user,"key":key,"status":status,"payload":order_payload}) #insert order 
        
        print(f"Converted user {fake_user} cart to order {order_id}....")

    all_eventss=[ add_to_cart,update_in_cart, delete_from_cart ,checkout ]

    event_seq=0
    while(event_seq<50):
        revent=random.choice(all_eventss)
        revent(client, table_name)
        event_seq=event_seq+1
        time.sleep(3)
        print('------------')


        
if(__name__=='__main__'):

    TABLE_NAME='EcommerceCartsAndOrders'
    in_cart=[] # global cart list to maintain
    client=boto3.client('dynamodb',region_name='ap-south-1')
    
    if(len(sys.argv)==2 and sys.argv[1]=="create_table_only"):

        print("Only creating table if it does not exist! ")
        status=does_table_exist(client,TABLE_NAME)

        if(status==False):
            create_table(client,TABLE_NAME)

    else:
        generate_fake_event(client,TABLE_NAME)
    