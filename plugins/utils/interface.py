from abc import ABC, abstractmethod

class DataPipelineInterface(ABC):
    """Data Lake ~ Warehouse 전 구간을 포괄하는 표준 인터페이스"""

    @abstractmethod
    def fetch(self, **kwargs):
        """데이터 원천(API, 파일 등)에서 수집"""
        pass

    @abstractmethod
    def load(self, records, **kwargs):
        """수집된 데이터를 데이터베이스/스토리지에 적재"""
        pass

    @abstractmethod
    def validate(self, **kwargs):
        """적재 후 데이터 정합성 검사"""
        pass

    def transform(self, **kwargs):
        """필요 시 변환 로직 추가 (옵션)"""
        return None
