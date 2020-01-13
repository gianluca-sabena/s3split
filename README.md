# s3split

s3split splits big datasets in different tar archives and uploads/downloads them to/from s3 remote storage

s3split features:

- splits datasets in different tar archive with a max size
- splits files snd folder only on the first level (different dataset parts can be consumed independently)
- uploads of split parts in parallel
- generates index file

## Run

- Activate env `pipenv shell`
- Install dependencies `pipenv install`
- Run main `python src/s3split.py -h`
- Run tests `pytest`

## Dev

- Install dev dependencies `pipenv install --dev`
- Created with `pipenv --python 3.7`
- Add a dev dependency `pipenv install pytest --dev`
- Add current module `s3split` in editable mode with `pipenv install --dev -e .`
- Run python and import s3split `pipenv run python -c 'import s3split.app; s3split.app.run_cli()'`
- Test `pytest` (requires docker to run a [minio](https://minio.io) s3 backend)

## Publish package

## Notes

- Packages and directory layout:
  - Separate src and test dir <https://docs.pytest.org/en/latest/goodpractices.html?highlight=directory%20layout#tests-outside-application-code>
  - see also a suggested link <https://blog.ionelmc.ro/2014/05/25/python-packaging/#the-structure>
  - python package <https://docs.python.org/3.7/tutorial/modules.html#packages>
- console script <https://python-packaging.readthedocs.io/en/latest/command-line-scripts.html>
- Add file `tests/conftest.py` to instruct pytest to add src dir to sys.path <https://stackoverflow.com/a/50156706/7568979>

## Docs

- s3 example
  - Boto3 docs <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-examples.html>
  - AWS python sdk <https://docs.aws.amazon.com/code-samples/latest/catalog/code-catalog-python-example_code-s3.html>
