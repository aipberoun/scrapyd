import traceback
import uuid, re
from cStringIO import StringIO

from twisted.python import log

from .utils import get_spider_list, JsonResource, UtilsCache
from ConfigParser import SafeConfigParser
from cStringIO import StringIO
from pkgutil import get_data
from collections import defaultdict

class WsResource(JsonResource):

    def __init__(self, root):
        JsonResource.__init__(self)
        self.root = root

    def render(self, txrequest):
        try:
            return JsonResource.render(self, txrequest)
        except Exception, e:
            if self.root.debug:
                return traceback.format_exc()
            log.err()
            r = {"node_name": self.root.nodename, "status": "error", "message": str(e)}
            return self.render_object(r, txrequest)

class Scrape(WsResource):

    def __init__(self, root):
        WsResource.__init__(self, root)
        cp = SafeConfigParser()
        projects_config = get_data(__package__, 'mapping.conf')
        cp.readfp(StringIO(projects_config))
        self.projects = cp.sections()
        self.spider_mappings = []
        for project in self.projects:
            # we concatenate all regexp : spider mappings and remember the project they belong to
            for regexp, spider in cp.items(project):
                self.spider_mappings.append( (regexp, project, spider) )

    def get_project_spider_list(self, url_list):
        res = defaultdict(lambda : [])
        not_matched = []
        for url in url_list:
            matched = False
            for regexp, project, spider in self.spider_mappings:
                if re.search(regexp, url):
                    res[project,spider] = res[project,spider] + [url.strip()]
                    matched = True
            if not matched:
                not_matched.append(url)
        return res, not_matched

    def handle_project_spider(self, args, project, spider, url_list):
        if not project in self.root.scheduler.list_projects():
            return {"status": "error", "message": "project %s not found" % project, "matched_urls": url_list}
        spiders = get_spider_list(project)
        if not spider in spiders:
            return {"status": "error", "message": "spider '%s' not found in project '%s'" % (spider, project), "matched_urls": url_list}
        jobid = uuid.uuid1().hex
        args['_job'] = jobid
        args['start_urls'] = "\n".join(url_list) # on its way to constructor this variable becomes string so we can't pass the list directly
        self.root.scheduler.schedule(project, spider, **args)
        return {"status": "ok", "jobid": jobid, "matched_urls": url_list, "project": project, "spider": spider}

    def render_POST(self, txrequest):
        settings = txrequest.args.pop('setting', [])
        settings = dict(x.split('=', 1) for x in settings)
        args = dict((k, v[0]) for k, v in txrequest.args.items())
        args['settings'] = settings
        ps_list, not_matched = self.get_project_spider_list(args.pop('urls').split(','))
        jobs = []
        errors = []
        for project, spider in ps_list:
            result = self.handle_project_spider(args, project, spider, ps_list[project, spider])
            if result['status'] == "ok":
                jobs.append(result)
            else:
                errors.append(result)
        return {"node_name": self.root.nodename, "jobs": jobs, "errors": errors, "not_matched": not_matched}

class Schedule(WsResource):

    def render_POST(self, txrequest):
        settings = txrequest.args.pop('setting', [])
        settings = dict(x.split('=', 1) for x in settings)
        args = dict((k, v[0]) for k, v in txrequest.args.items())
        project = args.pop('project')
        spider = args.pop('spider')
        spiders = get_spider_list(project)
        if not spider in spiders:
            return {"status": "error", "message": "spider '%s' not found" % spider}
        args['settings'] = settings
        jobid = uuid.uuid1().hex
        args['_job'] = jobid
        self.root.scheduler.schedule(project, spider, **args)
        return {"node_name": self.root.nodename, "status": "ok", "jobid": jobid}

class Cancel(WsResource):

    def render_POST(self, txrequest):
        args = dict((k, v[0]) for k, v in txrequest.args.items())
        project = args['project']
        jobid = args['job']
        signal = args.get('signal', 'TERM')
        prevstate = None
        queue = self.root.poller.queues[project]
        c = queue.remove(lambda x: x["_job"] == jobid)
        if c:
            prevstate = "pending"
        spiders = self.root.launcher.processes.values()
        for s in spiders:
            if s.job == jobid:
                s.transport.signalProcess(signal)
                prevstate = "running"
        return {"node_name": self.root.nodename, "status": "ok", "prevstate": prevstate}

class AddVersion(WsResource):

    def render_POST(self, txrequest):
        project = txrequest.args['project'][0]
        version = txrequest.args['version'][0]
        eggf = StringIO(txrequest.args['egg'][0])
        self.root.eggstorage.put(eggf, project, version)
        spiders = get_spider_list(project)
        self.root.update_projects()
        UtilsCache.invalid_cache(project)
        return {"node_name": self.root.nodename, "status": "ok", "project": project, "version": version, \
            "spiders": len(spiders)}

class ListProjects(WsResource):

    def render_GET(self, txrequest):
        projects = self.root.scheduler.list_projects()
        return {"node_name": self.root.nodename, "status": "ok", "projects": projects}

class ListVersions(WsResource):

    def render_GET(self, txrequest):
        project = txrequest.args['project'][0]
        versions = self.root.eggstorage.list(project)
        return {"node_name": self.root.nodename, "status": "ok", "versions": versions}

class ListSpiders(WsResource):

    def render_GET(self, txrequest):
        project = txrequest.args['project'][0]
        spiders = get_spider_list(project, runner=self.root.runner)
        return {"node_name": self.root.nodename, "status": "ok", "spiders": spiders}

class ListJobs(WsResource):

    def render_GET(self, txrequest):
        project = txrequest.args['project'][0]
        spiders = self.root.launcher.processes.values()
        running = [{"id": s.job, "spider": s.spider,
            "start_time": s.start_time.isoformat(' ')} for s in spiders if s.project == project]
        queue = self.root.poller.queues[project]
        pending = [{"id": x["_job"], "spider": x["name"]} for x in queue.list()]
        finished = [{"id": s.job, "spider": s.spider,
            "start_time": s.start_time.isoformat(' '),
            "end_time": s.end_time.isoformat(' ')} for s in self.root.launcher.finished
            if s.project == project]
        return {"node_name": self.root.nodename, "status":"ok", "pending": pending, "running": running, "finished": finished}

class DeleteProject(WsResource):

    def render_POST(self, txrequest):
        project = txrequest.args['project'][0]
        self._delete_version(project)
        return {"node_name": self.root.nodename, "status": "ok"}

    def _delete_version(self, project, version=None):
        self.root.eggstorage.delete(project, version)
        self.root.update_projects()

class DeleteVersion(DeleteProject):

    def render_POST(self, txrequest):
        project = txrequest.args['project'][0]
        version = txrequest.args['version'][0]
        self._delete_version(project, version)
        return {"node_name": self.root.nodename, "status": "ok"}
