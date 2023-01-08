def main():
    from relmd_types import PipelineHandling, SignalHandler

    def _now(offset_secs = 0):
        from datetime import datetime, timedelta
        now = datetime.now()
        now = now + timedelta(0, offset_secs)
        return now.strftime("%H:%M:%S")

    def get_default_log_path():
        from datetime import datetime, timedelta
        now = datetime.now()
        now = now.strftime("%Y%m%d%H%M%S")
        return f"/home/crawlers/log/crawlers_{now}.log"

    def main_command_template(module_path, class_key, class_value, start_tor_port = None, tor_proxies_count = None, **kwargs):
        cmd_args = []
        cmd_args.append(f"--module_path {module_path}")
        cmd_args.append(f"--{class_key} {class_value}")
        if start_tor_port and tor_proxies_count:
            cmd_args.append(f"--tor")
            cmd_args.append(f"--start_port={start_tor_port}")
            cmd_args.append(f"--tor_proxies_count={tor_proxies_count}")
        if "start_page" in kwargs:
            start_page = kwargs["start_page"]
            if start_page:
                cmd_args.append(f"--start_page={start_page}")
        if "pages_count" in kwargs:
            pages_count = kwargs["pages_count"]
            if pages_count:
                cmd_args.append(f"--pages_count={pages_count}")
        cmd_args = " ".join(cmd_args)
        cmd = f"python3 main.py {cmd_args}"
        return cmd

    def _scrapy_otodom(class_prefix, start_tor_port, tor_proxies_count, start_page = None):
        return main_command_template("relm.spiders.otodom.spiders", "class_prefix", f"{class_prefix}", start_tor_port, tor_proxies_count, start_page = None)

    def _run_process(process_cmd, **kwargs):
        global _processes
        from subprocess import Popen, DEVNULL
        print(f"{process_cmd}")
        was_stopped = False
        log_path = get_default_log_path()
        if "signal_handler" in kwargs:
            signal_handler = kwargs["signal_handler"]
            was_stopped = signal_handler.stopped()
        if "log_path" in kwargs:
            log_path = kwargs['log_path']
        proc = None
        with open(log_path, "w") as log_file:
            if not was_stopped:
                proc = Popen(f"{process_cmd}", shell=True, stdout=DEVNULL, stderr=log_file)
            if "signal_handler" in kwargs:
                signal_handler = kwargs["signal_handler"]
                signal_handler.register_proc(proc)
            if not was_stopped:
                proc.wait()
    
    def _scrapy_otodom_in_parts(class_prefix, start_tor_port, tor_proxies_count, start_page = 0, **kwargs):
        from relm.spiders.otodom.settings import OUTPUT_FILES
        import time
        def init_process():
            cmd = main_command_template("relm.spiders.otodom.spiders", "class_name", f"{class_prefix}InitData")
            _run_process(cmd, **kwargs)
            def spider_process():
                pages_count = None
                with open(OUTPUT_FILES[class_prefix], "r") as f:
                    import json
                    data = json.load(f)
                    pages_count = data["pages_count"]
                page_idx = start_page
                print(f"pages_count {pages_count}")
                from random import randint
                from time import sleep
                pages_in_session = 25
                for idx in range(start_page, pages_count, pages_in_session):
                    cmd = main_command_template("relm.spiders.otodom.spiders", "class_name", f"{class_prefix}Main", start_tor_port, tor_proxies_count, start_page = idx, pages_count = pages_in_session)
                    page_idx = idx
                    _run_process(cmd, **kwargs)
                    sleep(randint(20, 120))
                if pages_count - page_idx > 0:
                    offset = pages_count - page_idx
                    cmd = main_command_template("relm.spiders.otodom.spiders", "class_name", f"{class_prefix}Main", start_tor_port, tor_proxies_count, start_page = page_idx, pages_count = offset)
                    _run_process(cmd, **kwargs)
            spider_process()
        return init_process
    
    def _scrapy_otodom_real_estates_tor(start_tor_port, tor_count, start_page = None, **kwargs):
        return _scrapy_otodom("RealEstates", start_tor_port, tor_count, start_page, **kwargs)
    
    def _scrapy_otodom_build_plots_tor(start_tor_port, tor_count, start_page = None, **kwargs):
        return _scrapy_otodom("BuildPlots", start_tor_port, tor_count, start_page, **kwargs)
    
    def _scrapy_otodom_real_estates_tor_in_parts(start_tor_port, tor_count, start_page = 0, **kwargs):
        return _scrapy_otodom_in_parts("RealEstates", start_tor_port, tor_count, start_page, **kwargs)
    
    def _scrapy_otodom_build_plots_tor_in_parts(start_tor_port, tor_count, start_page = 0, **kwargs):
        return _scrapy_otodom_in_parts("BuildPlots", start_tor_port, tor_count, start_page, **kwargs)
    
    def _scrapy_otodom_tor_in_parts(start_tor_port, tor_count, start_page = 0):
        signal_handler = SignalHandler()
        def process():
            _scrapy_otodom_real_estates_tor_in_parts(start_tor_port, tor_count, start_page, signal_handler = signal_handler)()
            _scrapy_otodom_build_plots_tor_in_parts(start_tor_port, tor_count, start_page, signal_handler = signal_handler)()
        return signal_handler, process

    def _export_to_xslx():
        signal_handler = SignalHandler()
        def process():
            _run_process("python3 xslx_exporter.py", signal_handler = signal_handler)
        return signal_handler, process

    #pipelines = {(0, 100) : (PipelineHandling.RESTART, [_scrapy_otodom_tor_in_parts])}
    pipelines = {}
    pipelines[(1, 100)] = (PipelineHandling.CLEAR_RESTART, [_scrapy_otodom_tor_in_parts(9060, 7*2, 1)])
    pipelines[(2, 200)] = (PipelineHandling.CLEAR_RESTART, [_export_to_xslx()])
    return pipelines

pipelines = main()
