"""Versioned warehouse decision-context catalog; no LLM or graph writes."""
VERSION = 'warehouse-query-catalog-v1'


def __getattr__(name):
    if name == 'QueryCatalogService':
        from .service import QueryCatalogService
        return QueryCatalogService
    raise AttributeError(name)
