from azmeta.access.utils.sdk import default_sdk_client
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute.models import ResourceSku
from typing import Dict, NamedTuple, Callable, Any, Mapping, Optional, Iterable, List, Collection
from logging import Logger
import re


class VirtualMachineCapabilities(NamedTuple):
    acus: float # ACUs
    accelerated_networking_enabled: bool # AcceleratedNetworkingEnabled
    cached_disk_bytes: float # CachedDiskBytes
    combined_temp_disk_and_cached_iops: float # CombinedTempDiskAndCachedIOPS
    combined_temp_disk_and_cached_read_bytes_per_second: float # CombinedTempDiskAndCachedReadBytesPerSecond
    combined_temp_disk_and_cached_write_bytes_per_second: float # CombinedTempDiskAndCachedWriteBytesPerSecond
    ephemeral_os_disk_supported: bool # EphemeralOSDiskSupported
    gpus: float # GPUs
    hyperv_generations: str # HyperVGenerations
    low_priority_capable: bool # LowPriorityCapable
    max_data_disk_count: float # MaxDataDiskCount
    max_network_interfaces: float # MaxNetworkInterfaces
    max_resource_volume_mb: float # MaxResourceVolumeMB
    max_write_accelerator_disks_allowed: float # MaxWriteAcceleratorDisksAllowed
    memory_gb: float # MemoryGB
    os_vhd_size_mb: float # OSVhdSizeMB
    parent_size: str # ParentSize
    premium_io: bool # PremiumIO
    rdma_enabled: bool # RdmaEnabled
    uncached_disk_bytes_per_second: float # UncachedDiskBytesPerSecond
    uncached_disk_iops: float # UncachedDiskIOPS
    vcpus: float # vCPUs
    vcpus_available: float # vCPUsAvailable
    vcpus_per_core: float # vCPUsPerCore

    @property
    def d_total_acus(self):
        return self.acus * self.d_vcpus_available

    @property
    def d_vcpus_available(self):
        return self.vcpus_available if self.vcpus_available is not None else self.vcpus
    

class ManagedDiskCapabilities(NamedTuple):
    billing_partition_sizes: str # BillingPartitionSizes
    max_bandwidth_mbps: float # MaxBandwidthMBps
    max_bandwidth_mbps_read_only: float # MaxBandwidthMBpsReadOnly
    max_bandwidth_mbps_read_write: float # MaxBandwidthMBpsReadWrite
    max_io_size_kibps: float # MaxIOSizeKiBps
    max_iops: float # MaxIOps
    max_iops_read_write: float # MaxIOpsReadWrite
    max_iops_per_gib_read_only: float # MaxIopsPerGiBReadOnly
    max_iops_per_gib_read_write: float # MaxIopsPerGiBReadWrite
    max_iops_read_only: float # MaxIopsReadOnly
    max_size_gib: float # MaxSizeGiB
    min_bandwidth_mbps: float # MinBandwidthMBps
    min_bandwidth_mbps_read_only: float # MinBandwidthMBpsReadOnly
    min_bandwidth_mbps_read_write: float # MinBandwidthMBpsReadWrite
    min_io_size_kibps: float # MinIOSizeKiBps
    min_iops: float # MinIOps
    min_iops_read_write: float # MinIOpsReadWrite
    min_iops_per_gib_read_only: float # MinIopsPerGiBReadOnly
    min_iops_per_gib_read_write: float # MinIopsPerGiBReadWrite
    min_iops_read_only: float # MinIopsReadOnly
    min_size_gib: float # MinSizeGiB


class VirtualMachineSku(NamedTuple):
    tier: str
    family: str
    name: str
    size: str
    capabilities: VirtualMachineCapabilities


class ManagedDiskSku(NamedTuple):
    tier: str
    name: str
    size: str
    capabilities: ManagedDiskCapabilities


class AzureComputeSpecifications:
    def __init__(self):
        self._virtual_machine_skus: Dict[str, VirtualMachineSku]  = {}
        self._managed_disk_skus: Dict[str, ManagedDiskSku] = {}

    @property
    def virtual_machine_skus(self) -> Collection[VirtualMachineSku]:
        return self._virtual_machine_skus.values()
    
    @property
    def managed_disk_skus(self) -> Collection[ManagedDiskSku]:
        return self._managed_disk_skus.values()

    def virtual_machine_by_name(self, name: str) -> VirtualMachineSku:
        return self._virtual_machine_skus[name.lower()]

    def managed_disk_by_size(self, size: str) -> ManagedDiskSku:
        return self._managed_disk_skus[size.lower()]


def load_compute_specifications(logger: Logger) -> AzureComputeSpecifications:
    client = default_sdk_client(ComputeManagementClient)
    sku_pages: Iterable[ResourceSku] = client.resource_skus.list(filter="location eq 'eastus2'")
    specifications = AzureComputeSpecifications()
    for sku in sku_pages:
        if sku.resource_type not in ('virtualMachines', 'disks'):
            continue
        capabilities: Mapping[str,str] = {c.name: c.value for c in sku.capabilities}
        if sku.resource_type == 'virtualMachines':
            if sku.family == 'standardBSFamily' and 'ACUs' not in capabilities:
                capabilities['ACUs'] = 160

            # Bugs in data
            if sku.family in ('standardBSFamily', 'standardHBSFamily', 'standardHBrsv2Family', 'standardDCSv2Family', 'standardNCSv2Family', 'standardNCSv3Family', 'standardHCSFamily', 'standardNVSv3Family', 'standardNVSv4Family', 'standardNDSFamily', 'standardMSv2Family'):
                capabilities['EphemeralOSDiskSupported'] = 'False' 
            elif sku.family in ('standardDSv2PromoFamily', 'standardMSFamily'):
                capabilities['EphemeralOSDiskSupported'] = 'True' 
            
            match_constrained = re.search(r'-(\d+)', sku.name)
            if match_constrained is not None:
                constraint = float(match_constrained[1])
                vcpus = map_if_not_none(capabilities.get('vCPUs'), float)
                vcpus_available = map_if_not_none(capabilities.get('vCPUsAvailable'), float)
                if vcpus == vcpus_available or vcpus_available != constraint:
                    logger.warning(f'Auto-corrected likely incorrect data from ARM from SKU {sku.name}. vcpus: {vcpus} avail: {vcpus_available}')
                    capabilities['vCPUsAvailable'] = constraint

            if sku.name == 'Standard_E20_v3':
                capabilities['HyperVGenerations'] = 'V1,V2'
            
            if map_if_not_none(capabilities.get('PremiumIO'), _parse_bool) is False:
                if 'UncachedDiskBytesPerSecond' not in capabilities:
                    capabilities['UncachedDiskBytesPerSecond'] = 60 * 1024**2
                if 'UncachedDiskIOPS' not in capabilities:
                    capabilities['UncachedDiskIOPS'] = 500

            vm_capability_tuple = VirtualMachineCapabilities(
                acus = map_if_not_none(capabilities.get('ACUs'), float),
                accelerated_networking_enabled = map_if_not_none(capabilities.get('AcceleratedNetworkingEnabled'), _parse_bool),
                cached_disk_bytes = map_if_not_none(capabilities.get('CachedDiskBytes'), float),
                combined_temp_disk_and_cached_iops = map_if_not_none(capabilities.get('CombinedTempDiskAndCachedIOPS'), float),
                combined_temp_disk_and_cached_read_bytes_per_second = map_if_not_none(capabilities.get('CombinedTempDiskAndCachedReadBytesPerSecond'), float),
                combined_temp_disk_and_cached_write_bytes_per_second = map_if_not_none(capabilities.get('CombinedTempDiskAndCachedWriteBytesPerSecond'), float),
                ephemeral_os_disk_supported = map_if_not_none(capabilities.get('EphemeralOSDiskSupported'), _parse_bool),
                gpus = map_if_not_none(capabilities.get('GPUs'), float),
                hyperv_generations = capabilities.get('HyperVGenerations'), # type: ignore
                low_priority_capable = map_if_not_none(capabilities.get('LowPriorityCapable'), _parse_bool),
                max_data_disk_count = map_if_not_none(capabilities.get('MaxDataDiskCount'), float),
                max_network_interfaces = map_if_not_none(capabilities.get('MaxNetworkInterfaces'), float),
                max_resource_volume_mb = map_if_not_none(capabilities.get('MaxResourceVolumeMB'), float),
                max_write_accelerator_disks_allowed = map_if_not_none(capabilities.get('MaxWriteAcceleratorDisksAllowed'), float),
                memory_gb = map_if_not_none(capabilities.get('MemoryGB'), float),
                os_vhd_size_mb = map_if_not_none(capabilities.get('OSVhdSizeMB'), float),
                parent_size = capabilities.get('ParentSize'), # type: ignore
                premium_io = map_if_not_none(capabilities.get('PremiumIO'), _parse_bool),
                rdma_enabled = map_if_not_none(capabilities.get('RdmaEnabled'), _parse_bool),
                uncached_disk_bytes_per_second = map_if_not_none(capabilities.get('UncachedDiskBytesPerSecond'), float),
                uncached_disk_iops = map_if_not_none(capabilities.get('UncachedDiskIOPS'), float),
                vcpus = map_if_not_none(capabilities.get('vCPUs'), float),
                vcpus_available = map_if_not_none(capabilities.get('vCPUsAvailable'), float),
                vcpus_per_core = map_if_not_none(capabilities.get('vCPUsPerCore'), float)
            )
            key = sku.name.lower()
            existing_vm = specifications._virtual_machine_skus.get(key)
            if existing_vm is not None:
                assert existing_vm.capabilities == vm_capability_tuple
                #if not (existing_vm.capabilities == vm_capability_tuple):
                #    print(f"** OMG: {sku.name} - {sku.family} mistmatch")
                continue
            specifications._virtual_machine_skus[key] = VirtualMachineSku(sku.tier, sku.family, sku.name, sku.size, vm_capability_tuple)
        elif sku.resource_type == 'disks':   
            disk_capability_tuple = ManagedDiskCapabilities(
                billing_partition_sizes = capabilities.get('BillingPartitionSizes'), # type: ignore
                max_bandwidth_mbps = map_if_not_none(capabilities.get('MaxBandwidthMBps'), float),
                max_bandwidth_mbps_read_only = map_if_not_none(capabilities.get('MaxBandwidthMBpsReadOnly'), float),
                max_bandwidth_mbps_read_write = map_if_not_none(capabilities.get('MaxBandwidthMBpsReadWrite'), float),
                max_io_size_kibps = map_if_not_none(capabilities.get('MaxIOSizeKiBps'), float),
                max_iops = map_if_not_none(capabilities.get('MaxIOps'), float),
                max_iops_read_write = map_if_not_none(capabilities.get('MaxIOpsReadWrite'), float),
                max_iops_per_gib_read_only = map_if_not_none(capabilities.get('MaxIopsPerGiBReadOnly'), float),
                max_iops_per_gib_read_write = map_if_not_none(capabilities.get('MaxIopsPerGiBReadWrite'), float),
                max_iops_read_only = map_if_not_none(capabilities.get('MaxIopsReadOnly'), float),
                max_size_gib = map_if_not_none(capabilities.get('MaxSizeGiB'), float),
                min_bandwidth_mbps = map_if_not_none(capabilities.get('MinBandwidthMBps'), float),
                min_bandwidth_mbps_read_only = map_if_not_none(capabilities.get('MinBandwidthMBpsReadOnly'), float),
                min_bandwidth_mbps_read_write = map_if_not_none(capabilities.get('MinBandwidthMBpsReadWrite'), float),
                min_io_size_kibps = map_if_not_none(capabilities.get('MinIOSizeKiBps'), float),
                min_iops = map_if_not_none(capabilities.get('MinIOps'), float),
                min_iops_read_write = map_if_not_none(capabilities.get('MinIOpsReadWrite'), float),
                min_iops_per_gib_read_only = map_if_not_none(capabilities.get('MinIopsPerGiBReadOnly'), float),
                min_iops_per_gib_read_write = map_if_not_none(capabilities.get('MinIopsPerGiBReadWrite'), float),
                min_iops_read_only = map_if_not_none(capabilities.get('MinIopsReadOnly'), float),
                min_size_gib = map_if_not_none(capabilities.get('MinSizeGiB'), float)
            )
            key = sku.size.lower()
            existing_disk = specifications._managed_disk_skus.get(key)
            if existing_disk is not None:
                if sku.size not in ('E4', 'P4'): # these skus vary in their min size across locations for some reason.
                    assert existing_disk.capabilities == disk_capability_tuple
                continue
            specifications._managed_disk_skus[key] = ManagedDiskSku(sku.tier, sku.name, sku.size, disk_capability_tuple)
    
    return specifications


def map_if_not_none(value: Optional[str], mapper: Callable) -> Any:
    if value is None:
        return None
    return mapper(value)


def _parse_bool(value: str) -> bool:
    return value.lower() == 'true'