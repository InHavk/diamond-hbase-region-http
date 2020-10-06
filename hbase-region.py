# coding=utf-8

"""
Diamond collector for HBase Regionserver metrics
"""

from urllib2 import urlopen
import diamond.collector
import re
import json


def bean_metric(prefix):
    def big_wrapper(func):
        def wrapper(*args, **kwargs):
            itr = func(*args, **kwargs)
            while True:
                try:
                    path, value = itr.next()
                    if path.lower() == "modelertype" or \
                            path.lower().find("tag.") == 0 or \
                            path.lower() == "name" or \
                            path.lower() == "objectname":
                        continue
                    if not type(value) is int:
                        continue
                    path = path.replace(".", "_")
                    path = ".".join((prefix, path))
                    yield (path, value)
                except StopIteration:
                    break
        return wrapper
    return big_wrapper

re_region_metric = re.compile\
        (r'^(?P<prefix>.+)\.Namespace_(?P<namespace>.+)_table_(?P<table>.+)_region_(?P<region>.+)_metric_(?P<metric>.+)$')

re_table_metric = re.compile\
        (r'^(?P<prefix>.+)\.Namespace_(?P<namespace>.+)_table_(?P<table>.+)_metric_(?P<metric>.+)$')

def _split_metric(itr, r):
    while True:
        try:
            path, value = itr.next()
            reg = r.match(path)
            if not reg is None:
                parts_of_path = reg.groups()
                path = ".".join(parts_of_path)
            yield (path, value)
        except StopIteration:
            break

def split_region_metric(func):
    def wrapper(*args, **kwargs):
        itr = func(*args, **kwargs)
        for i in _split_metric(itr, re_region_metric):
            yield i
    return wrapper

def split_table_metric(func):
    def wrapper(*args, **kwargs):
        itr = func(*args, **kwargs)
        for i in _split_metric(itr, re_table_metric):
            yield i
    return wrapper


class HBaseRegionCollector(diamond.collector.Collector):

    def __init__(self, *args, **kwargs):
        self.BEANS_MAP = {
            "java.lang:type=Runtime": self.java_runtime,
            "java.lang:type=Threading": self.java_threading,
            "java.lang:type=OperatingSystem": self.java_operatingsystem,
            "Hadoop:service=HBase,name=MetricsSystem,sub=Stats": self.hbase_metricsystem_stats,
            "java.lang:type=MemoryPool,name=Code Cache": self.java_memorypool_codecache,
            "java.nio:type=BufferPool,name=direct": self.java_bufferpool_direct,
            "java.lang:type=GarbageCollector,name=G1 Young Generation": self.java_gc_G1_young,
            "java.lang:type=MemoryPool,name=G1 Old Gen": self.java_memorypool_G1_old,
            "java.lang:type=GarbageCollector,name=G1 Old Generation": self.java_gc_G1_old,
            "java.lang:type=MemoryPool,name=G1 Survivor Space": self.java_memorypool_G1_survivorspace,
            "java.lang:type=MemoryPool,name=Metaspace": self.java_memorypool_metaspace,
            "Hadoop:service=HBase,name=JvmMetrics": self.hbase_jvmmetrics,
            "java.lang:type=Memory": self.java_memory,
            "Hadoop:service=HBase,name=UgiMetrics": self.hbase_ugimetrics,
            "Hadoop:service=HBase,name=RegionServer,sub=Regions": self.hbase_region_regions,
            "Hadoop:service=HBase,name=RegionServer,sub=IO": self.hbase_region_io,
            "Hadoop:service=HBase,name=RegionServer,sub=Replication": self.hbase_region_replication,
            "Hadoop:service=HBase,name=RegionServer,sub=TableLatencies": self.hbase_region_tablelatencies,
            "Hadoop:service=HBase,name=RegionServer,sub=WAL": self.hbase_region_wal,
            "Hadoop:service=HBase,name=RegionServer,sub=Tables": self.hbase_region_tables,
            "Hadoop:service=HBase,name=RegionServer,sub=Server": self.hbase_region_server,
            "Hadoop:service=HBase,name=RegionServer,sub=IPC": self.hbase_region_ipc,
            "Hadoop:service=HBase,name=RegionServer,sub=PhoenixIndexer": self.hbase_region_phoenixindexer
        }
        super(HBaseRegionCollector, self).__init__(*args, **kwargs)

    def get_default_config_help(self):
        config_help = super(HBaseRegionCollector, self).get_default_config_help()
        config_help.update({
            'url': 'URL of jxm metrics',
            'metrics': 'List of beans name'
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(HBaseRegionCollector, self).get_default_config()
        config.update({
            'path':     'hbase.region',
            'url': 'http://127.0.0.1:60030/jmx',
            'metrics':  self.BEANS_MAP.keys()
        })
        return config

    def collect(self):
        url = self.config['url']
        try:
            response = urlopen(url)
            content = json.loads(response.read().decode())

            for bean in content['beans']:
                bean_name = bean['name']
                if not bean_name in self.config['metrics']:
                    continue
                func = self.BEANS_MAP[bean_name]
                for path, value in func(bean):
                    self.publish(path, value)
        except URLError:
            pass

    @bean_metric("runtime")
    def java_runtime(self, data):
        allowed = ("StartTime", "Uptime")
        for key, value in data.iteritems():
            if key in allowed:
                yield (key, value)

    @bean_metric("threading")
    def java_threading(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("operatingsystem")
    def java_operatingsystem(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("metricsystem_stats")
    def hbase_metricsystem_stats(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("memorypool_codecache")
    def java_memorypool_codecache(self, data):
        for key, value in data.iteritems():
            yield (key, value)
        for key in ("Usage", "PeakUsage"):
            for k, v in data[key].iteritems():
                yield (".".join((key, k)), v)

    @bean_metric("bufferpool_direct")
    def java_bufferpool_direct(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("gc_G1_young")
    def java_gc_G1_young(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("memorypool_G1_old")
    def java_memorypool_G1_old(self, data):
        for key, value in data.iteritems():
            yield (key, value)
        for key in ("Usage", "PeakUsage", "CollectionUsage"):
            for k, v in data[key].iteritems():
                yield (".".join((key, k)), v)

    @bean_metric("gc_G1_old")
    def java_gc_G1_old(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("memorypool_G1_survivorspace")
    def java_memorypool_G1_survivorspace(self, data):
        for key, value in data.iteritems():
            yield (key, value)
        for key in ("Usage", "PeakUsage", "CollectionUsage"):
            for k, v in data[key].iteritems():
                yield (".".join((key, k)), v)

    @bean_metric("memorypool_metaspace")
    def java_memorypool_metaspace(self, data):
        for key, value in data.iteritems():
            yield (key, value)
        for key in ("Usage", "PeakUsage"):
            for k, v in data[key].iteritems():
                yield (".".join((key, k)), v)

    @bean_metric("jvmmetrics")
    def hbase_jvmmetrics(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("memory")
    def java_memory(self, data):
        for key, value in data.iteritems():
            yield (key, value)
        for key in ("HeapMemoryUsage", "NonHeapMemoryUsage"):
            for k, v in data[key].iteritems():
                yield (".".join((key, k)), v)

    @bean_metric("ugimetrics")
    def hbase_ugimetrics(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @split_region_metric
    @bean_metric("regions")
    def hbase_region_regions(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("io")
    def hbase_region_io(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("replication")
    def hbase_region_replication(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @split_table_metric
    @bean_metric("tablelatencies")
    def hbase_region_tablelatencies(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("wal")
    def hbase_region_wal(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @split_table_metric
    @bean_metric("tables")
    def hbase_region_tables(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("server")
    def hbase_region_server(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("ipc")
    def hbase_region_ipc(self, data):
        for key, value in data.iteritems():
            yield (key, value)

    @bean_metric("phoenixindexer")
    def hbase_region_phoenixindexer(self, data):
        for key, value in data.iteritems():
            yield (key, value)
