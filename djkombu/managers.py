# Partially stolen from Django Queue Service
# (http://code.google.com/p/django-queue-service)
from django.conf import settings
from django.db import transaction, connection, models
try:
    from django.db import connections, router
except ImportError:  # pre-Django 1.2
    connections = router = None


class QueueManager(models.Manager):

    def publish(self, queue_name, payload):
        queue, created = self.get_or_create(name=queue_name)
        queue.messages.create(payload=payload)

    def fetch(self, queue_name):
        try:
            queue = self.get(name=queue_name)
        except self.model.DoesNotExist:
            return

        return queue.messages.pop()

    def size(self, queue_name):
        return self.get(name=queue_name).messages.count()

    def purge(self, queue_name):
        try:
            queue = self.get(name=queue_name)
        except self.model.DoesNotExist:
            return

        messages = queue.messages.all()
        count = messages.count()
        messages.delete()
        return count


class MessageManager(models.Manager):
    messages_received = 0
    cleanup_every = 10

    def pop(self):
        try:
            resultset = self.filter(visible=True).order_by('sent_at', 'id')
            result = resultset[0:1].get()
            result.visible = False
            result.save()
            if not self.messages_received % self.cleanup_every:
                self.cleanup()
            return result.payload
        except self.model.DoesNotExist:
            pass

    def cleanup(self):
        self.filter(visible=False).delete()

    def cleanup(self):
        cursor = self.connection_for_write().cursor()
        cursor.execute("DELETE FROM %s WHERE visible=%%s" % (
                        self.model._meta.db_table, ), (False, ))
        transaction.commit_unless_managed()

    def connection_for_write(self):
        if connections:
            return connections[router.db_for_write(self.model)]
        return connection
