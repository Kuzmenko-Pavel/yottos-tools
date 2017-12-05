# encoding: utf-8

from amqplib import client_0_8 as amqp


class MQ(object):
    '''
    Класс отвечает за отправку сообщений в RabbitMQ.
    '''

    def __init__(self, host, user, password, virtual_host):
        self.host = host
        self.user = user
        self.password = password
        self.virtual_host = virtual_host

    def _get_worker_channel(self):
        ''' Подключается к брокеру mq '''
        conn = amqp.Connection(host=self.host,
                               userid=self.user,
                               password=self.password,
                               virtual_host=self.virtual_host,
                               insist=True)
        ch = conn.channel()
        ch.exchange_declare(exchange="getmyad", type="topic", durable=False, auto_delete=True)
        return ch

    def campaign_start(self, campaign_id):
        ''' Отправляет уведомление о запуске рекламной кампании ``campaign_id`` '''
        ch_worker = self._get_worker_channel()
        msg = amqp.Message(campaign_id)
        ch_worker.basic_publish(msg, exchange='getmyad', routing_key='campaign.start')
        ch_worker.close()
        print "AMQP Campaign start %s" % campaign_id

    def campaign_stop(self, campaign_id):
        ''' Отправляет уведомление об остановке рекламной кампании ``campaign_id`` '''
        ch_worker = self._get_worker_channel()
        msg = amqp.Message(campaign_id)
        ch_worker.basic_publish(msg, exchange='getmyad', routing_key='campaign.stop')
        ch_worker.close()
        print "AMQP Campaign stop %s" % campaign_id

    def campaign_update(self, campaign_id):
        ''' Отправляет уведомление об обновлении рекламной кампании ``campaign_id`` '''
        ch_worker = self._get_worker_channel()
        msg = amqp.Message(campaign_id)
        ch_worker.basic_publish(msg, exchange='getmyad', routing_key='campaign.update')
        ch_worker.close()
        print "AMQP Campaign update %s" % campaign_id

    def informer_update(self, informer_id):
        ''' Отправляет уведомление о том, что информер ``informer_id`` был изменён '''
        ch_worker = self._get_worker_channel()
        msg = amqp.Message(informer_id)
        ch_worker.basic_publish(msg, exchange='getmyad', routing_key='informer.update')
        ch_worker.close()
        print "AMQP Informer update %s" % informer_id

    def account_update(self, login):
        ''' Отправляет уведомление об изменении в аккаунте ``login`` '''
        ch_worker = self._get_worker_channel()
        msg = amqp.Message(login)
        ch_worker.basic_publish(msg, exchange='getmyad', routing_key='account.update')
        ch_worker.close()
        try:
            print "AMQP Account update %s" % login
        except Exception as e:
            print e

    def offer_delete(self, offer_Id, campaign_id):
        ''' Отправляет уведомление об удалении рекламного предложения '''
        ch_worker = self._get_worker_channel()
        msg = 'Offer:%s,Campaign:%s' % (offer_Id, campaign_id)
        msg = amqp.Message(msg)
        ch_worker.basic_publish(msg, exchange='getmyad', routing_key='advertise.delete')
        ch_worker.close()
        print "AMQP Delete Offer:%s,Campaign:%s" % (offer_Id, campaign_id)

    def offer_add(self, offer_Id, campaign_id):
        '''Отправляет уведомление об добавлении рекламного предложения '''
        ch_worker = self._get_worker_channel()
        msg = 'Offer:%s,Campaign:%s' % (offer_Id, campaign_id)
        msg = amqp.Message(msg)
        ch_worker.basic_publish(msg, exchange='getmyad', routing_key='advertise.update')
        ch_worker.close()
        print "AMQP Add Offer:%s,Campaign:%s" % (offer_Id, campaign_id)
