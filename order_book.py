from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Base, Order

engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()


def process_order(order):
    # Your code here
    buy_currency = order['buy_currency']
    sell_currency = order['sell_currency']
    buy_amount = order['buy_amount']
    sell_amount = order['sell_amount']
    sender_pk = order['sender_pk']
    receiver_pk = order['receiver_pk']
    implied_exchange_rate = buy_amount / sell_amount
    new_order_sell_rate= sell_amount/buy_amount

    order_obj = Order(sender_pk=order['sender_pk'], receiver_pk=order['receiver_pk'],
                      buy_currency=order['buy_currency'], sell_currency=order['sell_currency'],
                      buy_amount=order['buy_amount'], sell_amount=order['sell_amount'])
    session.add(order_obj)
    session.commit()

    new_order_ID=order_obj.id

    query = session.query(Order).filter(
        Order.filled == None, Order.buy_currency == sell_currency, Order.sell_currency == buy_currency
    )
    result = session.execute(query)
    amount=0
    id_order_matched=0;


    for order in result.scalars().all():
        existing_order_exchange_rate=order.sell_amount/order.buy_amount
        if existing_order_exchange_rate>=implied_exchange_rate:
            if order.sell_amount>amount:
                amount=sell_amount
                id_order_matched=order.id

    pass

    if id_order_matched!=0:
        existing_order_sell_amount = 0
        existing_order_buy_amount=0
        existing_order_sell_rate= 0
        existing_order_sender_pk = None
        existing_order_receiver_pk = None

        now = datetime.now()

        query1=session.query(Order).filter(Order.id==id_order_matched)
        result1 = session.execute(query1)
        for order in result1.scalars().all():
            order.filled=now
            order.counterparty_id=new_order_ID
            existing_order_sell_amount=order.sell_amount
            existing_order_buy_amount=order.buy_amount
            existing_order_sender_pk = order.sender_pk
            existing_order_receiver_pk = order.receiver_pk
            existing_order_sell_rate=existing_order_sell_amount/existing_order_buy_amount
            session.commit()

        query2 = session.query(Order).filter(Order.id == new_order_ID)
        result2 = session.execute(query2)
        for order in result2.scalars().all():
            order.filled = now
            order.counterparty_id = id_order_matched
            session.commit()
        child_order_obj=None

        if existing_order_sell_amount<buy_amount:
            final_sell_amount=existing_order_sell_amount
            final_buy_amount=existing_order_buy_amount

            buy_amount = buy_amount-final_buy_amount
            sell_amount = buy_amount*new_order_sell_rate
            creator_id = new_order_ID

            child_order = {}
            child_order['sender_pk'] = sender_pk
            child_order['receiver_pk'] = receiver_pk
            child_order['buy_currency'] = buy_currency
            child_order['sell_currency'] = sell_currency
            child_order['buy_amount'] = buy_amount
            child_order['sell_amount'] = sell_amount
            child_order['creator_id'] = creator_id


            child_order_obj = Order(sender_pk=child_order['sender_pk'],
                                    receiver_pk=child_order['receiver_pk'],
                                    buy_currency=child_order['buy_currency'],
                                    sell_currency=child_order['sell_currency'],
                                    buy_amount=child_order['buy_amount'],
                                    sell_amount=child_order['sell_amount'],
                                    creator_id=child_order['creator_id'])
            session.add(child_order_obj)
            session.commit()

        elif existing_order_sell_amount>buy_amount:
            final_sell_amount=sell_amount
            final_buy_amount=buy_amount

            buy_currency_original=buy_currency
            sender_pk = existing_order_sender_pk
            receiver_pk = existing_order_receiver_pk
            buy_currency = sell_currency
            sell_currency = buy_currency_original
            buy_amount = existing_order_buy_amount - final_sell_amount
            sell_amount = buy_amount*existing_order_sell_rate
            creator_id = id_order_matched

            child_order = {}
            child_order['sender_pk'] = sender_pk
            child_order['receiver_pk'] = receiver_pk
            child_order['buy_currency'] = buy_currency
            child_order['sell_currency'] = sell_currency
            child_order['buy_amount']=buy_amount
            child_order['sell_amount'] = sell_amount
            child_order['creator_id'] = creator_id

            child_order_obj = Order(sender_pk=child_order['sender_pk'],
                                    receiver_pk=child_order['receiver_pk'],
                                    buy_currency=child_order['buy_currency'],
                                    sell_currency=child_order['sell_currency'],
                                    buy_amount=child_order['buy_amount'],
                                    sell_amount=child_order['sell_amount'],
                                    creator_id=child_order['creator_id'])

            session.add(child_order_obj)
            session.commit()
