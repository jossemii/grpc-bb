# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: celaut.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0c\x63\x65laut.proto\x12\x06\x63\x65laut\"\xe5\x04\n\x08\x46ieldDef\x12.\n\x07message\x18\x01 \x01(\x0b\x32\x1b.celaut.FieldDef.MessageDefH\x00\x12\x32\n\tprimitive\x18\x02 \x01(\x0b\x32\x1d.celaut.FieldDef.PrimitiveDefH\x00\x12(\n\x04\x65num\x18\x03 \x01(\x0b\x32\x18.celaut.FieldDef.EnumDefH\x00\x1a,\n\x0cPrimitiveDef\x12\x12\n\x05regex\x18\x01 \x01(\tH\x00\x88\x01\x01\x42\x08\n\x06_regex\x1ak\n\x07\x45numDef\x12\x32\n\x05value\x18\x01 \x03(\x0b\x32#.celaut.FieldDef.EnumDef.ValueEntry\x1a,\n\nValueEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x05:\x02\x38\x01\x1a\xa6\x02\n\nMessageDef\x12\x35\n\x05param\x18\x01 \x03(\x0b\x32&.celaut.FieldDef.MessageDef.ParamEntry\x12\x33\n\x05oneof\x18\x02 \x03(\x0b\x32$.celaut.FieldDef.MessageDef.OneofDef\x1a=\n\x08ParamDef\x12\x1f\n\x05\x66ield\x18\x01 \x01(\x0b\x32\x10.celaut.FieldDef\x12\x10\n\x08repeated\x18\x02 \x01(\x08\x1a\x19\n\x08OneofDef\x12\r\n\x05index\x18\x01 \x03(\x05\x1aR\n\nParamEntry\x12\x0b\n\x03key\x18\x01 \x01(\x05\x12\x33\n\x05value\x18\x02 \x01(\x0b\x32$.celaut.FieldDef.MessageDef.ParamDef:\x02\x38\x01\x42\x07\n\x05value\"\xc5\x03\n\x03\x41ny\x12+\n\x08metadata\x18\x01 \x01(\x0b\x32\x14.celaut.Any.MetadataH\x00\x88\x01\x01\x12\r\n\x05value\x18\x02 \x01(\x0c\x1a\xf4\x02\n\x08Metadata\x12\x32\n\x07hashtag\x18\x01 \x01(\x0b\x32\x1c.celaut.Any.Metadata.HashTagH\x00\x88\x01\x01\x12%\n\x06\x66ormat\x18\x02 \x01(\x0b\x32\x10.celaut.FieldDefH\x01\x88\x01\x01\x1a\xf5\x01\n\x07HashTag\x12/\n\x04hash\x18\x01 \x03(\x0b\x32!.celaut.Any.Metadata.HashTag.Hash\x12\x0b\n\x03tag\x18\x02 \x03(\t\x12>\n\x0c\x61ttr_hashtag\x18\x03 \x03(\x0b\x32(.celaut.Any.Metadata.HashTag.AttrHashTag\x1a#\n\x04Hash\x12\x0c\n\x04type\x18\x01 \x01(\x0c\x12\r\n\x05value\x18\x02 \x01(\x0c\x1aG\n\x0b\x41ttrHashTag\x12\x0b\n\x03key\x18\x01 \x01(\x05\x12+\n\x05value\x18\x02 \x03(\x0b\x32\x1c.celaut.Any.Metadata.HashTagB\n\n\x08_hashtagB\t\n\x07_formatB\x0b\n\t_metadata\"\xe2\x0f\n\x07Service\x12,\n\tcontainer\x18\x01 \x01(\x0b\x32\x19.celaut.Service.Container\x12 \n\x03\x61pi\x18\x02 \x01(\x0b\x32\x13.celaut.Service.Api\x12&\n\x06tensor\x18\x03 \x01(\x0b\x32\x16.celaut.Service.Tensor\x12&\n\x06ledger\x18\x04 \x01(\x0b\x32\x16.celaut.Service.Ledger\x1a\x81\x04\n\x03\x41pi\x12\x30\n\x0c\x61pp_protocol\x18\x01 \x01(\x0b\x32\x1a.celaut.Service.Api.AppDef\x12&\n\x04slot\x18\x02 \x03(\x0b\x32\x18.celaut.Service.Api.Slot\x12;\n\x0f\x63ontract_ledger\x18\x03 \x03(\x0b\x32\".celaut.Service.Api.ContractLedger\x1a\xe5\x01\n\x06\x41ppDef\x12\x36\n\x06method\x18\x01 \x03(\x0b\x32&.celaut.Service.Api.AppDef.MethodEntry\x1aN\n\tMethodDef\x12\x1f\n\x05input\x18\x01 \x01(\x0b\x32\x10.celaut.FieldDef\x12 \n\x06output\x18\x02 \x01(\x0b\x32\x10.celaut.FieldDef\x1aS\n\x0bMethodEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x33\n\x05value\x18\x02 \x01(\x0b\x32$.celaut.Service.Api.AppDef.MethodDef:\x02\x38\x01\x1a\x30\n\x04Slot\x12\x0c\n\x04port\x18\x01 \x01(\x05\x12\x1a\n\x12transport_protocol\x18\x02 \x01(\x0c\x1aI\n\x0e\x43ontractLedger\x12\x10\n\x08\x63ontract\x18\x01 \x01(\x0c\x12\x15\n\rcontract_addr\x18\x02 \x01(\t\x12\x0e\n\x06ledger\x18\x03 \x01(\t\x1a\xb4\x06\n\tContainer\x12\x14\n\x0c\x61rchitecture\x18\x01 \x01(\x0c\x12\x12\n\nfilesystem\x18\x02 \x01(\x0c\x12P\n\x14\x65nviroment_variables\x18\x03 \x03(\x0b\x32\x32.celaut.Service.Container.EnviromentVariablesEntry\x12\x12\n\nentrypoint\x18\x04 \x03(\t\x12\x30\n\x06\x63onfig\x18\x05 \x01(\x0b\x32 .celaut.Service.Container.Config\x12\x43\n\x10\x65xpected_gateway\x18\x06 \x01(\x0b\x32).celaut.Service.Container.ExpectedGateway\x1a\xa6\x02\n\nFilesystem\x12?\n\x06\x62ranch\x18\x01 \x03(\x0b\x32/.celaut.Service.Container.Filesystem.ItemBranch\x1a\xd6\x01\n\nItemBranch\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x0e\n\x04\x66ile\x18\x02 \x01(\x0cH\x00\x12\x44\n\x04link\x18\x03 \x01(\x0b\x32\x34.celaut.Service.Container.Filesystem.ItemBranch.LinkH\x00\x12:\n\nfilesystem\x18\x04 \x01(\x0b\x32$.celaut.Service.Container.FilesystemH\x00\x1a \n\x04Link\x12\x0b\n\x03src\x18\x01 \x01(\t\x12\x0b\n\x03\x64st\x18\x02 \x01(\tB\x06\n\x04item\x1a\x38\n\x06\x43onfig\x12\x0c\n\x04path\x18\x01 \x03(\t\x12 \n\x06\x66ormat\x18\x02 \x01(\x0b\x32\x10.celaut.FieldDef\x1ao\n\x0f\x45xpectedGateway\x12\x38\n\x14gateway_app_protocol\x18\x01 \x01(\x0b\x32\x1a.celaut.Service.Api.AppDef\x12\"\n\x1agateway_transport_protocol\x18\x02 \x03(\x0c\x1aL\n\x18\x45nviromentVariablesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x1f\n\x05value\x18\x02 \x01(\x0b\x32\x10.celaut.FieldDef:\x02\x38\x01\x1a\x88\x01\n\x06Tensor\x12\x30\n\x05index\x18\x01 \x03(\x0b\x32!.celaut.Service.Tensor.IndexEntry\x12\x0c\n\x04rank\x18\x02 \x01(\x05\x1a>\n\nIndexEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x1f\n\x05value\x18\x02 \x01(\x0b\x32\x10.celaut.FieldDef:\x02\x38\x01\x1a\xf0\x02\n\x06Ledger\x12:\n\rclass_diagram\x18\x01 \x01(\x0b\x32#.celaut.Service.Ledger.ClassDiagram\x12\x1f\n\x12\x63onsensus_protocol\x18\x02 \x01(\x0cH\x00\x88\x01\x01\x1a\xf1\x01\n\x0c\x43lassDiagram\x12?\n\x06\x63lases\x18\x01 \x03(\x0b\x32/.celaut.Service.Ledger.ClassDiagram.ClasesEntry\x1a@\n\x0bRelationDef\x12\x1f\n\x05\x66ield\x18\x01 \x01(\x0b\x32\x10.celaut.FieldDef\x12\x10\n\x08relation\x18\x02 \x01(\t\x1a^\n\x0b\x43lasesEntry\x12\x0b\n\x03key\x18\x01 \x01(\x05\x12>\n\x05value\x18\x02 \x01(\x0b\x32/.celaut.Service.Ledger.ClassDiagram.RelationDef:\x02\x38\x01\x42\x15\n\x13_consensus_protocol\"\xc0\x01\n\x08Instance\x12 \n\x03\x61pi\x18\x01 \x01(\x0b\x32\x13.celaut.Service.Api\x12+\n\x08uri_slot\x18\x02 \x03(\x0b\x32\x19.celaut.Instance.Uri_Slot\x1a\x1f\n\x03Uri\x12\n\n\x02ip\x18\x01 \x01(\t\x12\x0c\n\x04port\x18\x02 \x01(\x05\x1a\x44\n\x08Uri_Slot\x12\x15\n\rinternal_port\x18\x01 \x01(\x05\x12!\n\x03uri\x18\x02 \x03(\x0b\x32\x14.celaut.Instance.Uri\"\xac\x01\n\rConfiguration\x12L\n\x14\x65nviroment_variables\x18\x01 \x03(\x0b\x32..celaut.Configuration.EnviromentVariablesEntry\x12\x11\n\tspec_slot\x18\x02 \x03(\x05\x1a:\n\x18\x45nviromentVariablesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x0c:\x02\x38\x01\"\x91\x01\n\x11\x43onfigurationFile\x12!\n\x07gateway\x18\x01 \x01(\x0b\x32\x10.celaut.Instance\x12%\n\x06\x63onfig\x18\x02 \x01(\x0b\x32\x15.celaut.Configuration\x12\x32\n\x14initial_sysresources\x18\x03 \x01(\x0b\x32\x14.celaut.Sysresources\"\xd6\x01\n\x0cSysresources\x12\x19\n\x0c\x62lkio_weight\x18\x01 \x01(\x04H\x00\x88\x01\x01\x12\x17\n\ncpu_period\x18\x02 \x01(\x04H\x01\x88\x01\x01\x12\x16\n\tcpu_quota\x18\x03 \x01(\x04H\x02\x88\x01\x01\x12\x16\n\tmem_limit\x18\x04 \x01(\x04H\x03\x88\x01\x01\x12\x17\n\ndisk_space\x18\x05 \x01(\x04H\x04\x88\x01\x01\x42\x0f\n\r_blkio_weightB\r\n\x0b_cpu_periodB\x0c\n\n_cpu_quotaB\x0c\n\n_mem_limitB\r\n\x0b_disk_spaceb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'celaut_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _FIELDDEF_ENUMDEF_VALUEENTRY._options = None
  _FIELDDEF_ENUMDEF_VALUEENTRY._serialized_options = b'8\001'
  _FIELDDEF_MESSAGEDEF_PARAMENTRY._options = None
  _FIELDDEF_MESSAGEDEF_PARAMENTRY._serialized_options = b'8\001'
  _SERVICE_API_APPDEF_METHODENTRY._options = None
  _SERVICE_API_APPDEF_METHODENTRY._serialized_options = b'8\001'
  _SERVICE_CONTAINER_ENVIROMENTVARIABLESENTRY._options = None
  _SERVICE_CONTAINER_ENVIROMENTVARIABLESENTRY._serialized_options = b'8\001'
  _SERVICE_TENSOR_INDEXENTRY._options = None
  _SERVICE_TENSOR_INDEXENTRY._serialized_options = b'8\001'
  _SERVICE_LEDGER_CLASSDIAGRAM_CLASESENTRY._options = None
  _SERVICE_LEDGER_CLASSDIAGRAM_CLASESENTRY._serialized_options = b'8\001'
  _CONFIGURATION_ENVIROMENTVARIABLESENTRY._options = None
  _CONFIGURATION_ENVIROMENTVARIABLESENTRY._serialized_options = b'8\001'
  _globals['_FIELDDEF']._serialized_start=25
  _globals['_FIELDDEF']._serialized_end=638
  _globals['_FIELDDEF_PRIMITIVEDEF']._serialized_start=179
  _globals['_FIELDDEF_PRIMITIVEDEF']._serialized_end=223
  _globals['_FIELDDEF_ENUMDEF']._serialized_start=225
  _globals['_FIELDDEF_ENUMDEF']._serialized_end=332
  _globals['_FIELDDEF_ENUMDEF_VALUEENTRY']._serialized_start=288
  _globals['_FIELDDEF_ENUMDEF_VALUEENTRY']._serialized_end=332
  _globals['_FIELDDEF_MESSAGEDEF']._serialized_start=335
  _globals['_FIELDDEF_MESSAGEDEF']._serialized_end=629
  _globals['_FIELDDEF_MESSAGEDEF_PARAMDEF']._serialized_start=457
  _globals['_FIELDDEF_MESSAGEDEF_PARAMDEF']._serialized_end=518
  _globals['_FIELDDEF_MESSAGEDEF_ONEOFDEF']._serialized_start=520
  _globals['_FIELDDEF_MESSAGEDEF_ONEOFDEF']._serialized_end=545
  _globals['_FIELDDEF_MESSAGEDEF_PARAMENTRY']._serialized_start=547
  _globals['_FIELDDEF_MESSAGEDEF_PARAMENTRY']._serialized_end=629
  _globals['_ANY']._serialized_start=641
  _globals['_ANY']._serialized_end=1094
  _globals['_ANY_METADATA']._serialized_start=709
  _globals['_ANY_METADATA']._serialized_end=1081
  _globals['_ANY_METADATA_HASHTAG']._serialized_start=813
  _globals['_ANY_METADATA_HASHTAG']._serialized_end=1058
  _globals['_ANY_METADATA_HASHTAG_HASH']._serialized_start=950
  _globals['_ANY_METADATA_HASHTAG_HASH']._serialized_end=985
  _globals['_ANY_METADATA_HASHTAG_ATTRHASHTAG']._serialized_start=987
  _globals['_ANY_METADATA_HASHTAG_ATTRHASHTAG']._serialized_end=1058
  _globals['_SERVICE']._serialized_start=1097
  _globals['_SERVICE']._serialized_end=3115
  _globals['_SERVICE_API']._serialized_start=1269
  _globals['_SERVICE_API']._serialized_end=1782
  _globals['_SERVICE_API_APPDEF']._serialized_start=1428
  _globals['_SERVICE_API_APPDEF']._serialized_end=1657
  _globals['_SERVICE_API_APPDEF_METHODDEF']._serialized_start=1494
  _globals['_SERVICE_API_APPDEF_METHODDEF']._serialized_end=1572
  _globals['_SERVICE_API_APPDEF_METHODENTRY']._serialized_start=1574
  _globals['_SERVICE_API_APPDEF_METHODENTRY']._serialized_end=1657
  _globals['_SERVICE_API_SLOT']._serialized_start=1659
  _globals['_SERVICE_API_SLOT']._serialized_end=1707
  _globals['_SERVICE_API_CONTRACTLEDGER']._serialized_start=1709
  _globals['_SERVICE_API_CONTRACTLEDGER']._serialized_end=1782
  _globals['_SERVICE_CONTAINER']._serialized_start=1785
  _globals['_SERVICE_CONTAINER']._serialized_end=2605
  _globals['_SERVICE_CONTAINER_FILESYSTEM']._serialized_start=2062
  _globals['_SERVICE_CONTAINER_FILESYSTEM']._serialized_end=2356
  _globals['_SERVICE_CONTAINER_FILESYSTEM_ITEMBRANCH']._serialized_start=2142
  _globals['_SERVICE_CONTAINER_FILESYSTEM_ITEMBRANCH']._serialized_end=2356
  _globals['_SERVICE_CONTAINER_FILESYSTEM_ITEMBRANCH_LINK']._serialized_start=2316
  _globals['_SERVICE_CONTAINER_FILESYSTEM_ITEMBRANCH_LINK']._serialized_end=2348
  _globals['_SERVICE_CONTAINER_CONFIG']._serialized_start=2358
  _globals['_SERVICE_CONTAINER_CONFIG']._serialized_end=2414
  _globals['_SERVICE_CONTAINER_EXPECTEDGATEWAY']._serialized_start=2416
  _globals['_SERVICE_CONTAINER_EXPECTEDGATEWAY']._serialized_end=2527
  _globals['_SERVICE_CONTAINER_ENVIROMENTVARIABLESENTRY']._serialized_start=2529
  _globals['_SERVICE_CONTAINER_ENVIROMENTVARIABLESENTRY']._serialized_end=2605
  _globals['_SERVICE_TENSOR']._serialized_start=2608
  _globals['_SERVICE_TENSOR']._serialized_end=2744
  _globals['_SERVICE_TENSOR_INDEXENTRY']._serialized_start=2682
  _globals['_SERVICE_TENSOR_INDEXENTRY']._serialized_end=2744
  _globals['_SERVICE_LEDGER']._serialized_start=2747
  _globals['_SERVICE_LEDGER']._serialized_end=3115
  _globals['_SERVICE_LEDGER_CLASSDIAGRAM']._serialized_start=2851
  _globals['_SERVICE_LEDGER_CLASSDIAGRAM']._serialized_end=3092
  _globals['_SERVICE_LEDGER_CLASSDIAGRAM_RELATIONDEF']._serialized_start=2932
  _globals['_SERVICE_LEDGER_CLASSDIAGRAM_RELATIONDEF']._serialized_end=2996
  _globals['_SERVICE_LEDGER_CLASSDIAGRAM_CLASESENTRY']._serialized_start=2998
  _globals['_SERVICE_LEDGER_CLASSDIAGRAM_CLASESENTRY']._serialized_end=3092
  _globals['_INSTANCE']._serialized_start=3118
  _globals['_INSTANCE']._serialized_end=3310
  _globals['_INSTANCE_URI']._serialized_start=3209
  _globals['_INSTANCE_URI']._serialized_end=3240
  _globals['_INSTANCE_URI_SLOT']._serialized_start=3242
  _globals['_INSTANCE_URI_SLOT']._serialized_end=3310
  _globals['_CONFIGURATION']._serialized_start=3313
  _globals['_CONFIGURATION']._serialized_end=3485
  _globals['_CONFIGURATION_ENVIROMENTVARIABLESENTRY']._serialized_start=3427
  _globals['_CONFIGURATION_ENVIROMENTVARIABLESENTRY']._serialized_end=3485
  _globals['_CONFIGURATIONFILE']._serialized_start=3488
  _globals['_CONFIGURATIONFILE']._serialized_end=3633
  _globals['_SYSRESOURCES']._serialized_start=3636
  _globals['_SYSRESOURCES']._serialized_end=3850
# @@protoc_insertion_point(module_scope)
