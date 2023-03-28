from dkg.module import Module
from dkg.method import Method
from dkg.utils.node_request import NodeRequest, validate_operation_status
from dkg.dataclasses import NodeResponseDict
from dkg.utils.decorators import retry
from dkg.exceptions import OperationNotFinished
from dkg.manager import DefaultRequestManager
from dkg.types import JSONLD, NQuads
from dkg.exceptions import DatasetOutputFormatNotSupported
from dkg.utils.rdf import normalize_dataset
from typing import Literal
from rdflib.plugins.sparql.parser import parseQuery
from pyld import jsonld


class Graph(Module):
    def __init__(self, manager: DefaultRequestManager):
        self.manager = manager

    _query = Method(NodeRequest.query)
    _get_operation_result = Method(NodeRequest.get_operation_result)

    def query(
        self,
        query: str,
        repository: str,
        output_format: Literal['JSON-LD', 'N-Quads'] = 'JSON-LD'
    ) -> JSONLD | NQuads:
        parsed_query = parseQuery(query)
        query_type = parsed_query[1].name.replace('Query', '').upper()

        operation_id: NodeResponseDict = self._query(query, query_type, repository)['operationId']
        operation_result = self.get_operation_result(operation_id, 'query')

        match output_format.lower():
            case 'json-ld' | 'jsonld':
                return jsonld.from_rdf(operation_result['data'])
            case 'n-quads' | 'nquads':
                return normalize_dataset(operation_result['data'], 'N-Quads')
            case _:
                raise DatasetOutputFormatNotSupported(
                    f'Dataset input format isn\'t supported: {output_format}. '
                    'Supported formats: JSON-LD / N-Quads.'
                )

    _get_operation_result = Method(NodeRequest.get_operation_result)

    @retry(catch=OperationNotFinished, max_retries=5, base_delay=1, backoff=2)
    def get_operation_result(self, operation_id: str, operation: str) -> NodeResponseDict:
        operation_result = self._get_operation_result(
            operation_id=operation_id,
            operation=operation,
        )

        validate_operation_status(operation_result)

        return operation_result
