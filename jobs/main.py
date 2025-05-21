import importlib
from typing import Any


def to_camel_case(s: str) -> str:
    return ''.join(word.capitalize() for word in s.split('_'))


def get_job_class(job_path: str) -> Any:
    """ """
    module = importlib.import_module(f'jobs.{job_path}.job')
    last_part = job_path.split('.')[-1]
    class_name = to_camel_case(last_part) + 'Job'

    return getattr(module, class_name)


def main() -> None:
    import sys

    job_name = sys.argv[1]
    ref_date = sys.argv[2]

    job_class = get_job_class(job_name)

    config = {'ref_date': ref_date}

    job_instance = job_class(config=config)
    job_instance.run()
    return


if __name__ == '__main__':
    main()
