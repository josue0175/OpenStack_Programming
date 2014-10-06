'''
Created on 21 Feb 2014

@author: walshh2
'''
#from oslo.config import cfg
#from cinder.volume import volume_types
#from cinder.openstack.common import log as logging
import time
import logging
# from cinderclient.v1 import client
LOG = logging.getLogger(__name__)
try:
    import pywbem
except ImportError:
    LOG.info(_('Module PyWBEM not installed.  '
               'Install PyWBEM using the python-pywbem package.'))
    
EMC_ROOT = 'root/emc'
PROVISIONING = 'storagetype:provisioning'
POOL = 'storagetype:pool'
protocol = 'iscsi'

def get_provisioning():
    # provisioning is thin (5) by default
    provisioning = 5 
    return provisioning

def find_storage_configuration_service(storage_system, conn):
    foundConfigService = None
    configservices = conn.EnumerateInstanceNames(
        'EMC_StorageConfigurationService')
    for configservice in configservices:
        if storage_system == configservice['SystemName']:
            foundConfigService = configservice
            LOG.debug("Found Storage Configuration Service: %s", str(configservice))
            break
    
    return foundConfigService

# CIM_ProtocolControllerMaskingCapabilities

def find_ProtocolControllerMaskingCapabilities(conn):
    maskingCapabilities = conn.EnumerateInstanceNames(
        'CIM_ProtocolControllerMaskingCapabilities') 
    return maskingCapabilities

def find_MaskingViews(conn):
    
    maskingview_name = 'VMAXCE_APPLIANCE_DS_MV'
    maskingviews = conn.EnumerateInstanceNames(
            'EMC_LunMaskingSCSIProtocolController')
    
    
    
    for view in maskingviews:
        instance = conn.GetInstance(view, LocalOnly=False)
        elementName = instance['ElementName']
        print elementName
        if maskingview_name == instance['ElementName']:
            foundview = view
            break
    
    groups = conn.AssociatorNames(
        foundview,
        ResultClass='SE_DeviceMaskingGroup')
    foundMaskingGroup = groups[0]
        
    return foundMaskingGroup

def find_StorageGroup(conn, storageGroup_name):
    
    foundStorageGroup = None
    storageGroups = conn.EnumerateInstanceNames(
            'SE_DeviceMaskingGroup')
    
    
    
    for storageGroup in storageGroups:
        instance = conn.GetInstance(storageGroup, LocalOnly=False)
        elementName = instance['ElementName']
        print elementName
        if storageGroup_name == instance['ElementName']:
            foundStorageGroup = storageGroup
            break
    
        
    return foundStorageGroup

def find_PortGroup(conn, portGroup_name):
    
    foundPortGroup = None
    portGroups = conn.EnumerateInstanceNames(
            'SE_TargetMaskingGroup')
    
    
    
    for portGroup in portGroups:
        instance = conn.GetInstance(portGroup, LocalOnly=False)
        elementName = instance['ElementName']
        print elementName
        if portGroup_name == instance['ElementName']:
            foundPortGroup = portGroup
            break
    
        
    return foundPortGroup
    


def find_InitiatorGroup(conn, initiatorGroup_name):
    
    foundInitiatorGroup = None
    initiatorGroups = conn.EnumerateInstanceNames(
            'SE_InitiatorMaskingGroup')
    
    
    
    
    
    for initiatorGroup in initiatorGroups:
        print initiatorGroup 
        hardwareIds = get_initators_from_initiator_group(conn, initiatorGroup);
        
        for hardwareId in hardwareIds:
            print hardwareId 
        instance = conn.GetInstance(initiatorGroup, LocalOnly=False)
        elementName = instance['ElementName']
        print elementName
        if initiatorGroup_name == instance['ElementName']:
            foundInitiatorGroup = initiatorGroup
            break
    
        
    return foundInitiatorGroup


def find_MaskingView(conn, maskingView_name):
    
    foundMaskingView = None
    maskingViews = conn.EnumerateInstanceNames(
            'Symm_LunMaskingView')
    
    
    
    for maskingView in maskingViews:
        instance = conn.GetInstance(maskingView, LocalOnly=False)
        elementName = instance['ElementName']
        print elementName
        if maskingView_name == instance['ElementName']:
            foundMaskingView = maskingView
            break
    
        
    return foundMaskingView


def find_SCSIProtocolControllers(conn):
    
    scsiProtocolControllers = conn.EnumerateInstanceNames(
            'CIM_SCSIProtocolController')
        
    return scsiProtocolControllers;

def find_SCSIProtocolEndPoints(conn):
    
    scsiProtocolEndPoints = conn.EnumerateInstanceNames(
            'CIM_SCSIProtocolEndpoint')
        
    return scsiProtocolEndPoints;

def find_FCSCSIProtocolEndpoints(conn):
    
    FCSCSIProtocolEndpoint = conn.EnumerateInstanceNames(
            'Symm_FCSCSIProtocolEndpoint')
        
    return FCSCSIProtocolEndpoint;


def find_iSCSIProtocolEndpoint(conn):
    
    iSCSIProtocolEndpoint = conn.EnumerateInstanceNames(
            'Symm_iSCSIProtocolEndpoint')
        
    return iSCSIProtocolEndpoint;

def find_StorageHardwareIDs(conn):
    
    StorageHardwareIDs = conn.EnumerateInstanceNames(
            'CIM_StorageHardwareID')
        
    return StorageHardwareIDs;





def find_LogicalDevices(conn):
    
    LogicalDevices = conn.EnumerateInstanceNames(
            'CIM_LogicalDevice')
        
    return LogicalDevices;

def find_StorageVolumes(conn):
    
    storageVolumes = conn.EnumerateInstanceNames(
            'EMC_StorageVolume')
    
#     for storageVolume in storageVolumes:
#         
#         associators = conn.Associators(
#             storageVolume,
#             resultClass='SNIA_VolumeView')
#         for assoc in associators:   
#             userFriendlyName = assoc['SVOtherIdentifyingInfo']      
#             print userFriendlyName
        
    return storageVolumes;


def find_MappedVolumes(conn, storageVolumeInstances):
    
    maskingViewMatrix = [[]]
    mappedVolumesMatrix = [[]]
    
    
    
    for vol_instance in storageVolumeInstances:
        
        unitnames = conn.ReferenceNames(
            vol_instance,
            ResultClass='CIM_ProtocolControllerForUnit')
        
        for unitname in unitnames:
            controller = unitname['Antecedent']
            classname = controller['CreationClassName']
            
            index = classname.find('Symm_LunMaskingView')
            if index > -1:  # VMAX
                deviceId = vol_instance['DeviceID']
                maskingViewMatrix.append([vol_instance, controller])
                print 'adding to maskingViewList %s', deviceId
            index = classname.find('Symm_MappingSCSIProtocolController')
            if index > -1:  # VMAX
                deviceId = vol_instance['DeviceID']
                mappedVolumesMatrix.append([vol_instance, controller])
                print 'adding to mappedVolumesList %s', deviceId
                
                

#         if out_num_device_number is None:
#             LOG.info(_("Device number not found for volume "
#                      "%(volumename)s %(vol_instance)s.") %
#                      {'volumename': volumename,
#                       'vol_instance': str(vol_instance.path)})
#         else:
#             LOG.debug(_("Found device number %(device)d for volume "
#                       "%(volumename)s %(vol_instance)s.") %
#                       {'device': out_num_device_number,
#                        'volumename': volumename,
#                        'vol_instance': str(vol_instance.path)})
# 
#         data = {'hostlunid': out_num_device_number,
#                 'storagesystem': storage_system,
#                 'owningsp': sp}
# 
#         LOG.debug(_("Device info: %(data)s.") % {'data': data})

    return maskingViewMatrix, mappedVolumesMatrix
        



def find_LunMaskingSCSIProtocolControllers(conn):
    
    LunMaskingSCSIProtocolControllers = conn.EnumerateInstanceNames(
            'Symm_LunMaskingSCSIProtocolController')
        
    return LunMaskingSCSIProtocolControllers;


def find_AuthorizedPrivilege(conn):
    
    AuthorizedPrivileges = conn.EnumerateInstanceNames(
            'CIM_AuthorizedPrivilege')
        
    return AuthorizedPrivileges;


def find_ProtocolControllerForUnit(conn):
    
    ProtocolControllerForUnit = conn.EnumerateInstanceNames(
            'CIM_ProtocolControllerForUnit')
        
    return ProtocolControllerForUnit;


def find_AuthorizedSubjects(conn):
    
    AuthorizedSubjects = conn.EnumerateInstanceNames(
            'CIM_AuthorizedSubject')
        
    return AuthorizedSubjects;


def find_AuthorizedTargets(conn):
    
    Authorized_Targets = conn.EnumerateInstanceNames(
            'CIM_AuthorizedTarget')
        
    return Authorized_Targets;


def find_SAPAvailableForElements(conn):
    
    SAPAvailableForElements = conn.EnumerateInstanceNames(
            'CIM_SAPAvailableForElement')
        
    return SAPAvailableForElements;


def find_ControllerConfigurationService(conn):
    
    foundConfigService = None
    controllerConfigurationServices = conn.EnumerateInstanceNames(
            'Symm_ControllerConfigurationService')
    
    for configservice in controllerConfigurationServices:
        foundConfigService = configservice
        #LOG.debug(_("Found Storage Configuration Service: %s")
        #          % (str(configservice)))
        break
        
    return foundConfigService;


    
def get_ecom_connection():
#     conn = pywbem.WBEMConnection('https://10.108.246.160', ('admin', '#1Password'),
#                                      default_namespace='root/emc')
#     conn = pywbem.WBEMConnection('https://10.108.246.206', ('admin', '#1Password'),
#                                      default_namespace='root/emc')
    conn = pywbem.WBEMConnection('http://10.108.246.202', ('admin', '#1Password'),
                                     default_namespace='root/emc')
    if conn is None:
        LOG.error(_("Cannot connect to ECOM server"))
        raise
    return conn

def parse_pool_instance_id(instanceid):
    # Example of pool InstanceId: CLARiiON+APM00115204878+U+Pool 0
    poolname = None
    systemname = None
    endp = instanceid.rfind('+')
    if endp > -1:
        poolname = instanceid[endp + 1:]

    idarray = instanceid.split('+')
    if len(idarray) > 2:
        systemname = idarray[0] + '+' + idarray[1]

#     logging.debug(_("Pool name: %(poolname)s  System name: %(systemname)s.")
#               % {'poolname': poolname, 'systemname': systemname})
    return poolname, systemname

def find_pool(conn):
    
    foundPool = None
    systemname = None
    vpools = conn.EnumerateInstanceNames(
                'EMC_VirtualProvisioningPool')
    upools = conn.EnumerateInstanceNames(
                'EMC_UnifiedStoragePool')
    
    for upool in upools:
        poolinstance = upool['InstanceID']
        poolname, systemname = parse_pool_instance_id(poolinstance)
        if poolname is not None and systemname is not None:
            foundPool = upool
            break
    if foundPool is None:
        for vpool in vpools:
            poolinstance = vpool['InstanceID']
            print poolinstance
        for vpool in vpools:
            poolinstance = vpool['InstanceID']
            poolname, systemname = parse_pool_instance_id(
                    poolinstance)
            if poolname is not None and systemname is not None:
                foundPool = vpool
                break
    if foundPool is None:
        exception_message = (_("Pool %(storage_type)s is not found."))
        LOG.error(exception_message)
        raise 

    if systemname is None:
        exception_message = (_("Storage system not found for pool "
                                 "%(storage_type)s."))
        LOG.error(exception_message)
        raise 

#     logging.debug(_("Pool: %(pool)s  SystemName: %(systemname)s.")
#                   % {'pool': str(foundPool), 'systemname': systemname})
    return foundPool, systemname

def find_storageSystem(conn, array):
    storageSystem = None
    storageSystems = conn.EnumerateInstanceNames(
                'EMC_StorageSystem')
    for storageSystem in storageSystems:
        arrayName = storageSystem['Name']
        index = arrayName.find(array)
        if index > -1:
            return storageSystem
    return storageSystem
    


def find_pool_in_array(conn, array):
    
    foundPool = None
    systemname = None
    
    storageSystem = find_storageSystem(conn, array)
    
    vpools = conn.Associators(storageSystem,resultClass='EMC_VirtualProvisioningPool')
    
    upools = conn.Associators(storageSystem,resultClass='EMC_UnifiedStoragePool')
    
    for upool in upools:
        poolinstance = upool['InstanceID']
        poolname, systemname = parse_pool_instance_id(poolinstance)
        if poolname is not None and systemname is not None:
            foundPool = upool
            break
    if foundPool is None:
        for vpool in vpools:
            poolinstance = vpool['InstanceID']
            print poolinstance
        for vpool in vpools:
            poolinstance = vpool['InstanceID']
            poolname, systemname = parse_pool_instance_id(
                    poolinstance)
            if poolname is not None and systemname is not None:
                foundPool = vpool
                break
    if foundPool is None:
        exception_message = (_("Pool %(storage_type)s is not found."))
        LOG.error(exception_message)
        raise 

    if systemname is None:
        exception_message = (_("Storage system not found for pool "
                                 "%(storage_type)s."))
        LOG.error(exception_message)
        raise 

#     logging.debug(_("Pool: %(pool)s  SystemName: %(systemname)s.")
#                   % {'pool': str(foundPool), 'systemname': systemname})
    return foundPool, systemname

def _wait_for_job_complete(conn, job):
    jobinstancename = job['Job']
    
    while True:
        jobinstance = conn.GetInstance(jobinstancename,
                                            LocalOnly=False)
        jobstate = jobinstance['JobState']
        # From ValueMap of JobState in CIM_ConcreteJob
        # 2L=New, 3L=Starting, 4L=Running, 32767L=Queue Pending
        # ValueMap("2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13..32767,
        # 32768..65535"),
        # Values("New, Starting, Running, Suspended, Shutting Down,
        # Completed, Terminated, Killed, Exception, Service,
        # Query Pending, DMTF Reserved, Vendor Reserved")]
        if jobstate in [2L, 3L, 4L, 32767L]:
            time.sleep(10)
        else:
            break
    
    rc = jobinstance['ErrorCode']
    errordesc = jobinstance['ErrorDescription']
    
    return rc, errordesc

def getnum(num, datatype):
    try:
        result = {
            '8': pywbem.Uint8(num),
            '16': pywbem.Uint16(num),
            '32': pywbem.Uint32(num),
            '64': pywbem.Uint64(num)
        }
        result = result.get(datatype, num)
    except NameError:
        result = num
    
    return result

def wait_for_job_complete(conn, job):
    jobinstancename = job['Job']
    
    while True:
        jobinstance = conn.GetInstance(jobinstancename,
                                            LocalOnly=False)
        jobstate = jobinstance['JobState']
        # From ValueMap of JobState in CIM_ConcreteJob
        # 2L=New, 3L=Starting, 4L=Running, 32767L=Queue Pending
        # ValueMap("2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13..32767,
        # 32768..65535"),
        # Values("New, Starting, Running, Suspended, Shutting Down,
        # Completed, Terminated, Killed, Exception, Service,
        # Query Pending, DMTF Reserved, Vendor Reserved")]
        if jobstate in [2L, 3L, 4L, 32767L]:
            time.sleep(10)
        else:
            break
    
    rc = jobinstance['ErrorCode']
    errordesc = jobinstance['ErrorDescription']
    
    return rc, errordesc

 
def create_volume(conn, foundPool, configService, provisioning, volumeName, volumeSize):    
    rc, job = conn.InvokeMethod(
            'CreateOrModifyElementFromStoragePool',
            configService, ElementName=volumeName, InPool=foundPool,
            ElementType=getnum(provisioning, '16'),
            Size=getnum(volumeSize, '64')) 
    
    if rc != 0L:
        rc, errordesc = wait_for_job_complete(conn, job)
        if rc != 0L:
            LOG.error(_('Error Create Volume: %(volumename)s.  '
                      'Return code: %(rc)lu.  Error: %(error)s')
                      % {'volumename': volumeName,
                         'rc': rc,
                         'error': errordesc})
            raise 
    return rc, job


'''given a list of initiator names find the SE_StorageHardwareID instance'''
def _get_storage_hardware_id(conn, configService, initiatorNames):
    
#     storageHardwareIDs = conn.EnumerateInstanceNames(
#             'SE_StorageHardwareID')
    
    associators =\
            conn.Associators(configService,
                            resultClass='EMC_StorageHardwareID')
        
    return associators;


'''attempt to create a storage group.  If it fails because it already exists then attempt to add it to the storage group'''
def create_or_add_to_storageGroup(conn, configService, groupName, groupType, vol_instance):    
    try:
        rc, job = conn.InvokeMethod(
                'CreateGroup',
                configService,
                GroupName=groupName, 
                Type=getnum(groupType, '16'), 
                Members=[vol_instance.path]) 
        
        if rc != 0L:
            rc, errordesc = wait_for_job_complete(conn, job)
            if rc != 0L:
                LOG.error(_('Error Create Group: %(groupName)s.  '
                          'Return code: %(rc)lu.  Error: %(error)s')
                          % {'groupName': groupName,
                             'rc': rc,
                             'error': errordesc})
                raise 
    except pywbem.cim_operations.CIMError, e:
        #if the exception contains "Cannot use the specified name because it's already in use"
        exStr = str(e)
        print exStr
        s = "already in use"
        if exStr.find(s) == -1:
            #raise the exception that really did except.
            raise pywbem.cim_operations.CIMError
        else:
            #The storage group aleady exists, add the volume to the storage group
            print 'adding the volume to the storage group' 
            storage_group = find_StorageGroup(conn, groupName)
            add_volume_storage_group(conn, configService, vol_instance, storage_group)
            
    return rc, job

'''attempt to create a initiatorGroup.  If one already exists with the same Initiator/wwns then get it'''
def _create_or_get_initiator_group(conn, configService, groupName, groupType, connector):  
    
    #Check to see if an initiatorGroup already exists, that matches the connector information
    #NOTE:  An initiator/wwn can only belong to one initiatorGroup.  If we were to attempt to create 
    #one with an initiator/wwn that is already belong to another initiatorGroup, it would fail
    
    initiatorNames = _find_initiator_names(conn, connector)
    foundInitiatorGroup = _find_initiator_masking_group(conn, configService, initiatorNames)  
    
    #if you cannot find an initiatorGroup that matches the connector info create a new initiatorGroup
    if foundInitiatorGroup is None:
        #check that our connector information matches the hardwareId(s) on the symm
        try:
            
            storageHardwareIDInstances = _get_storage_hardware_id(conn, configService, initiatorNames)
            
            rc, job = conn.InvokeMethod(
                    'CreateGroup',
                    configService,
                    GroupName=groupName, 
                    Type=getnum(groupType, '16'), 
                    Members=[storageHardwareIDInstances]) 
            
            if rc != 0L:
                rc, errordesc = wait_for_job_complete(conn, job)
                if rc != 0L:
                    LOG.error(_('Error Create Group: %(groupName)s.  '
                              'Return code: %(rc)lu.  Error: %(error)s')
                              % {'groupName': groupName,
                                 'rc': rc,
                                 'error': errordesc})
                    raise 
        except pywbem.cim_operations.CIMError, e:
            #if the exception contains "Cannot use the specified name because it's already in use"
            exStr = str(e)
            print exStr
            s = "already in use"
            if exStr.find(s) == -1:
                #raise the exception that really did except.
                raise pywbem.cim_operations.CIMError
            else:
                #The storage group already exists, add the volume to the storage group
                print 'adding the volume to the storage group' 
                storage_group = find_StorageGroup(conn, groupName)
                add_volume_storage_group(conn, configService, connector, storage_group)
        
    else:
        print foundInitiatorGroup

    
            
    return rc, job

def create_portGroup(conn, configService, groupName, groupType,fcScsi_instances):    
    rc, job = conn.InvokeMethod(
            'CreateGroup',
            configService,
            GroupName=groupName, 
            Type=getnum(groupType, '16'),   
            Members=[fcScsi_instances[0],fcScsi_instances[1]] ) 
    
    if rc != 0L:
        rc, errordesc = wait_for_job_complete(conn, job)
        if rc != 0L:
            LOG.error(_('Error Create Group: %(groupName)s.  '
                      'Return code: %(rc)lu.  Error: %(error)s')
                      % {'groupName': groupName,
                         'rc': rc,
                         'error': errordesc})
            raise 
    return rc, job


def create_initiatorGroup(conn, configService, groupName, groupType, storageHardware_instances):    
    rc, job = conn.InvokeMethod(
            'CreateGroup',
            configService,
            GroupName=groupName, 
            Type=getnum(groupType, '16'),   
            Members=[storageHardware_instances[0]] ) 
    
    if rc != 0L:
        rc, errordesc = wait_for_job_complete(conn, job)
        if rc != 0L:
            LOG.error(_('Error Create Group: %(groupName)s.  '
                      'Return code: %(rc)lu.  Error: %(error)s')
                      % {'groupName': groupName,
                         'rc': rc,
                         'error': errordesc})
            raise 
    return rc, job


def create_masking_view(conn, configService, maskingViewName, deviceMaskingGroup, targetMaskingGroup, initiatorMaskingGroup):    
    rc, job = conn.InvokeMethod(
            'CreateMaskingView',
            configService,
            ElementName=maskingViewName, 
            InitiatorMaskingGroup=initiatorMaskingGroup,   
            DeviceMaskingGroup=deviceMaskingGroup,
            TargetMaskingGroup=targetMaskingGroup ) 
    
    if rc != 0L:
        rc, errordesc = wait_for_job_complete(conn, job)
        if rc != 0L:
            LOG.error(_('Error Create Masking View: %(maskingViewName)s.  '
                      'Return code: %(rc)lu.  Error: %(error)s')
                      % {'groupName': maskingViewName,
                         'rc': rc,
                         'error': errordesc})
            raise 
    return rc, job

def find_new_volume(conn, job): 
    # Find the newly created volume
    associators = conn.Associators(
        job['Job'],
        resultClass='EMC_StorageVolume')
    volpath = associators[0].path
    name = {}
    name['classname'] = volpath.classname
    keys = {}
    keys['CreationClassName'] = volpath['CreationClassName']
    keys['SystemName'] = volpath['SystemName']
    keys['DeviceID'] = volpath['DeviceID']
    keys['SystemCreationClassName'] = volpath['SystemCreationClassName']
    name['keybindings'] = keys
    return name
    
    
    
def find_new_masking_view(conn, job): 
    # Find the newly created volume
    associators = conn.Associators(
        job['Job'],
        resultClass='Symm_LunMaskingView')
    mvpath = associators[0].path
    name = {}
    name['classname'] = mvpath.classname
    keys = {}
    keys['CreationClassName'] = mvpath['CreationClassName']
    keys['SystemName'] = mvpath['SystemName']
    keys['DeviceID'] = mvpath['DeviceID']
    keys['SystemCreationClassName'] = mvpath['SystemCreationClassName']
    name['keybindings'] = keys   
    return name
    
    
#     LOG.debug(_('Leaving create_volume: %(volumename)s  '
#               'Return code: %(rc)lu '
#               'volume instance: %(name)s')
#               % {'volumename': volumename,
#                  'rc': rc,
#                  'name': name})
    
    return name

def delete_volume(conn, configService, vol_instance):
    device_id = vol_instance['DeviceID']
    print device_id
    rc, job = conn.InvokeMethod('EMCReturnToStoragePool',
                                   configService,
                                   TheElements=[vol_instance])
    if rc != 0L:
        rc, errordesc = wait_for_job_complete(conn,job)
        if rc != 0L:
#             exception_message = (_('Error Delete Volume: %(volumename)s.  '
#                                  'Return code: %(rc)lu.  Error: %(error)s')
#                                  % {'device_id': device_id,
#                                     'rc': rc,
#                                     'error': errordesc})
#             LOG.error(exception_message)
#             raise
            print errordesc
    return rc

def getinstancename(classname, bindings):
    instancename = None
    try:
        instancename = pywbem.CIMInstanceName(
            classname,
            namespace=EMC_ROOT,
            keybindings=bindings)
    except NameError:
        instancename = None

    return instancename
  

def find_lun(conn, volume, volumename):
    instancename = getinstancename(volume['classname'],
                                   volume['keybindings'])
    foundinstance = conn.GetInstance(instancename)

    if foundinstance is None:
        LOG.debug(_("Volume %(volumename)s not found on the array.")
                  % {'volumename': volumename})
#     else:
#         LOG.debug(_("Volume name: %(volumename)s  Volume instance: "
#                   "%(vol_instance)s.")
#                   % {'volumename': volumename,
#                      'vol_instance': str(foundinstance.path)})

    return foundinstance  

def getAssociatedInitiatorMaskingGroup(conn, maskingview):
    
    initiatorMaskingGroups = conn.AssociatorNames(
        maskingview,
        ResultClass='CIM_AssociatedInitiatorMaskingGroup')
    
    return initiatorMaskingGroups

# Find a device number that a host can see for a volume
def find_device_number(conn, volume):
    out_num_device_number = None

    volumename = volume['name']
    vol_instance = find_lun(volume)
    storage_system = vol_instance['SystemName']
    sp = None
    try:
        sp = vol_instance['EMCCurrentOwningStorageProcessor']
    except KeyError:
        # VMAX LUN doesn't have this property
        pass

    unitnames = conn.ReferenceNames(
        vol_instance.path,
        ResultClass='CIM_ProtocolControllerForUnit')

    for unitname in unitnames:
        controller = unitname['Antecedent']
        classname = controller['CreationClassName']
        index = classname.find('LunMaskingSCSIProtocolController')
        if index > -1:  # VNX
            # Get an instance of CIM_ProtocolControllerForUnit
            unitinstance = conn.GetInstance(unitname,
                                                 LocalOnly=False)
            numDeviceNumber = int(unitinstance['DeviceNumber'], 16)
            out_num_device_number = numDeviceNumber
            break
        else:
            index = classname.find('Symm_LunMaskingView')
            if index > -1:  # VMAX
                unitinstance = conn.GetInstance(unitname,
                                                     LocalOnly=False)
                numDeviceNumber = int(unitinstance['DeviceNumber'], 16)
                out_num_device_number = numDeviceNumber
                break

#     if out_num_device_number is None:
#         LOG.info(_("Device number not found for volume "
#                  "%(volumename)s %(vol_instance)s.") %
#                  {'volumename': volumename,
#                   'vol_instance': str(vol_instance.path)})
#     else:
#         LOG.debug(_("Found device number %(device)d for volume "
#                   "%(volumename)s %(vol_instance)s.") %
#                   {'device': out_num_device_number,
#                    'volumename': volumename,
#                    'vol_instance': str(vol_instance.path)})

    data = {'hostlunid': out_num_device_number,
            'storagesystem': storage_system,
            'owningsp': sp}

#     LOG.debug(_("Device info: %(data)s.") % {'data': data})

    return data


def initialize_connection(conn, volume, connector):
    """Initializes the connection and returns connection info."""
    volumename = volume['name']
#     LOG.info(_('Initialize connection: %(volume)s')
#              % {'volume': volumename})
    conn = get_ecom_connection()
    device_info = find_device_number(conn, volume)
    device_number = device_info['hostlunid']
    if device_number is not None:
        LOG.info(_("Volume %s is already mapped.")
                 % (volumename))
#     else:
#         self._map_lun(volume, connector)
#         # Find host lun id again after the volume is exported to the host
#         device_info = self.find_device_number(volume)

    return device_info


def find_device_masking_group(conn, maskingview_name):
    """Finds the Device Masking Group in a masking view."""
#     foundMaskingGroup = None

    maskingviews = conn.EnumerateInstanceNames(
        'EMC_LunMaskingSCSIProtocolController')
    for view in maskingviews:
        instance = conn.GetInstance(view, LocalOnly=False)
        if maskingview_name == instance['ElementName']:
            foundView = view
            break

    groups = conn.AssociatorNames(
        foundView,
        ResultClass='SE_DeviceMaskingGroup')
    foundMaskingGroup = groups[0]

#     LOG.debug(_("Masking view: %(view)s DeviceMaskingGroup: %(masking)s.")
#               % {'view': maskingview_name,
#                  'masking': str(foundMaskingGroup)})

    return foundMaskingGroup


def find_target_masking_group(conn, maskingview_name):
    """Finds the Target Masking Group in a masking view."""
#     foundMaskingGroup = None

    maskingviews = conn.EnumerateInstanceNames(
        'EMC_LunMaskingSCSIProtocolController')
    for view in maskingviews:
        instance = conn.GetInstance(view, LocalOnly=False)
        if maskingview_name == instance['ElementName']:
            foundView = view
            break

    groups = conn.AssociatorNames(
        foundView,
        ResultClass='SE_TargetMaskingGroup')
    foundMaskingGroup = groups[0]

#     LOG.debug(_("Masking view: %(view)s DeviceMaskingGroup: %(masking)s.")
#               % {'view': maskingview_name,
#                  'masking': str(foundMaskingGroup)})

    return foundMaskingGroup


def find_initiator_masking_group(conn, maskingview_name):
    """Finds the Initiator Masking Group in a masking view."""
#     foundMaskingGroup = None

    maskingviews = conn.EnumerateInstanceNames(
        'EMC_LunMaskingSCSIProtocolController')
    for view in maskingviews:
        instance = conn.GetInstance(view, LocalOnly=False)
        if maskingview_name == instance['ElementName']:
            foundView = view
            break

    groups = conn.AssociatorNames(
        foundView,
        ResultClass='SE_InitiatorMaskingGroup')
    foundMaskingGroup = groups[0]

#     LOG.debug(_("Masking view: %(view)s DeviceMaskingGroup: %(masking)s.")
#               % {'view': maskingview_name,
#                  'masking': str(foundMaskingGroup)})

    return foundMaskingGroup


def get_devices_from_storage_group(conn, storage_group):
    """Gets the existing volumes from the storagegroup"""
#     foundMaskingGroup = None


    volumes = conn.AssociatorNames(
        storage_group,
        ResultClass='CIM_StorageVolume')

#     LOG.debug(_("Masking view: %(view)s DeviceMaskingGroup: %(masking)s.")
#               % {'view': maskingview_name,
#                  'masking': str(foundMaskingGroup)})

    return volumes


def get_ports_from_port_group(conn, port_group):
    """Gets the existing ports from the portgroup"""
#     foundMaskingGroup = None


    ports = conn.AssociatorNames(
        port_group,
        ResultClass='CIM_SCSIProtocolEndpoint')

#     LOG.debug(_("Masking view: %(view)s DeviceMaskingGroup: %(masking)s.")
#               % {'view': maskingview_name,
#                  'masking': str(foundMaskingGroup)})

    return ports


def get_initators_from_initiator_group(conn, initator_group):
    """Gets the existing initiators from the initiatorGroup"""
#     foundMaskingGroup = None


    initiators = conn.AssociatorNames(
        initator_group,
        ResultClass='CIM_StorageHardwareID')

#     LOG.debug(_("Masking view: %(view)s DeviceMaskingGroup: %(masking)s.")
#               % {'view': maskingview_name,
#                  'masking': str(foundMaskingGroup)})

    return initiators

def add_volume_storage_group(conn, configservice, volume, storage_group, vol_instance):
    
    volumename = vol_instance['ElementName']
    
    rc, job =\
        conn.InvokeMethod('AddMembers',
                          configservice,
                          MaskingGroup=storage_group,
                          Members=[vol_instance.path])

    if rc != 0L:
        rc, errordesc = _wait_for_job_complete(conn,job)
        if rc != 0L:
            msg = (_('Error mapping volume %(vol)s. %(error)s') %
                   {'vol': volumename, 'error': errordesc})
            LOG.error(msg)
            raise

    print 'AddMembers completed successfully for volume ' +  volumename
    
    
def _find_initiator_names(conn, connector):
    foundinitiatornames = []
    iscsi = 'iscsi'
    fc = 'fc'
    name = 'initiator name'
    if protocol.lower() == iscsi and connector['initiator']:
        foundinitiatornames.append(connector['initiator'])
    elif protocol.lower() == fc and connector['wwpns']:
        for wwn in connector['wwpns']:
            foundinitiatornames.append(wwn)
        name = 'world wide port names'
    
    if foundinitiatornames is None or len(foundinitiatornames) == 0:
        msg = (_('Error finding %s.') % name)
        LOG.error(msg)
        raise 
    
#     LOG.debug(_("Found %(name)s: %(initiator)s.")
#               % {'name': name,
#                  'initiator': foundinitiatornames})
#     print 'Found' + foundinitiatornames
    return foundinitiatornames


'''Check to see if an initiatorGroup already exists, that matches the connector information
NOTE:  An initiator/wwn can only belong to one initiatorGroup.  If we were to attempt to create 
one with an initiator/wwn that is already belong to another initiatorGroup, it would fail'''

def _find_initiator_masking_group(conn, storageSystem, initiatorNames):
    foundInitiatorMaskingGroup = None
    
    initiatorMaskingGroups =\
            conn.Associators(storageSystem,
                                  resultClass='SE_InitiatorMaskingGroup')

    for initiatorMaskingGroup in initiatorMaskingGroups:
        associators =\
            conn.Associators(initiatorMaskingGroup.path,
                                  resultClass='EMC_StorageHardwareID')
        for assoc in associators:
            # if EMC_StorageHardwareID matches the initiator,
            # we found the existing EMC_LunMaskingSCSIProtocolController
            # (Storage Group for VNX)
            # we can use for masking a new LUN
            hardwareid = assoc['StorageID']
            for initiator in initiatorNames:
                print 'initiator is ' + initiator
                print 'hardwareId is ' + hardwareid
                if str(hardwareid).lower() == str(initiator).lower():
                    foundInitiatorMaskingGroup = initiatorMaskingGroup
                    break
    
            if foundInitiatorMaskingGroup is not None:
                break
    
        if foundInitiatorMaskingGroup is not None:
            break
        
#     print 'LunMaskingSCSIProtocolController for storage system' + storage_system + ' and initiator ' + initiators + ' is ' + str(foundCtrl)
    return foundInitiatorMaskingGroup
    
    
    
    
if __name__ == '__main__':
    connector = None
    volumeName = 'testVolume3'
    volumeSize = '1'
    array = '000198700440'
    hostname = 'myhost'
    
    
    
    #connection for ecom
    cliconn = get_ecom_connection()
    
    #connection for cinderclient
#     cinder = client.Client('os_admin', 'os1234', 'os_vmax_demo', 'http://10.108.246.211:5000/v2.0')
    
    
    foundPool, systemname = find_pool(cliconn)
     
#     storageVolumes = find_StorageVolumes(cliconn)
#     maskingViewMatrix, mappedVolumesMatrix = find_MappedVolumes(cliconn, storageVolumes)
#      
#     for mappedVolumes in mappedVolumesMatrix:
#         print mappedVolumes
#         if mappedVolumes is None:
#             print 'empty'
#         else:
#             for scsiProtocolControl in mappedVolumes: 
#                 classname = scsiProtocolControl['CreationClassName']
#                 index = classname.find('Symm_MappingSCSIProtocolController')
#                 if index > -1:  # VMAX
#                     initiatorMaskingGroups = getAssociatedInitiatorMaskingGroup(cliconn, scsiProtocolControl)
#          
#         
#         
#        
#     foundPool, systemname = find_pool(cliconn)
    print foundPool
    print systemname
    configService = find_storage_configuration_service(systemname, cliconn)
    print configService
    provisioning = get_provisioning()
    print provisioning
    
    sgGroupName = 'OS_'
    sgGroupName += foundPool['InstanceID']
    sgGroupName += '_'
    sgGroupName += hostname
    pgGroupName = 'Openstack_PG'
#     igGroupName = 'Openstack_IG'
    maskingViewName = 'Openstack_FC_MV'
    storageGroupType = 4;
    portGroupType = 3;
    initiatorGroupType = 2;
    
    connector = {'initiator': 'iqn.1996-04.de.suse:01:3def83d6831'}
    
    igGroupName = 'OS_'
    igGroupName += connector['initiator']
    
#     
    controllerConfigurationService = find_ControllerConfigurationService(cliconn)
    print controllerConfigurationService
    
#     rc, job = create_volume(cliconn, foundPool, configService, provisioning, volumeName, volumeSize)
#     volume  = find_new_volume(cliconn, job)
#     vol_instance = find_lun(cliconn, volume, volumeName)
#     print vol_instance
#     
#     connector ='blah'
#     
#     device_info = initialize_connection(cliconn, vol_instance, connector)
    
    
#     storageGroup = find_StorageGroup(cliconn, sgGroupName)
#     if storageGroup is None:
#     rc, job = create_volume(cliconn, foundPool, configService, provisioning, volumeName, volumeSize)
#     volume  = find_new_volume(cliconn, job)
#     vol_instance = find_lun(cliconn, volume, volumeName)
#     print vol_instance
#     rc, job = create_or_add_to_storageGroup(cliconn, controllerConfigurationService, sgGroupName, storageGroupType, vol_instance)
    
    
#     rc, job = create_or_get_initiator_group(cliconn, controllerConfigurationService, initiatorGroupType, connector)
#         
#         
#     portGroup = find_PortGroup(cliconn, pgGroupName)
#     if portGroup is None:
#         FCSCSIProtocolEndpoints = find_FCSCSIProtocolEndpoints(cliconn)
#         print FCSCSIProtocolEndpoints
#         rc, job = create_portGroup(cliconn, controllerConfigurationService, pgGroupName, portGroupType, FCSCSIProtocolEndpoints)
#         
#     initiatorGroup = find_InitiatorGroup(cliconn, igGroupName)
#     if initiatorGroup is None:
#         StorageHardwareIDs = find_StorageHardwareIDs(cliconn)
#         print StorageHardwareIDs
#         rc, job = create_initiatorGroup(cliconn, controllerConfigurationService, igGroupName, initiatorGroupType, StorageHardwareIDs)
    rc, job = _create_or_get_initiator_group(cliconn, controllerConfigurationService, igGroupName, initiatorGroupType, connector)
        
    
#     maskingView = find_MaskingView(cliconn, maskingViewName);
#     print maskingView
#     if maskingView is None:   
#         rc, job = create_masking_view(cliconn, controllerConfigurationService, maskingViewName, storageGroup, portGroup, initiatorGroup)
#         newMaskingView  = find_new_masking_view(cliconn, job)
#         print newMaskingView`


#     maskingViewName = 'Cluster_0003'
    storage_group = find_device_masking_group(cliconn, maskingViewName)
    print storage_group
    
    volumes = get_devices_from_storage_group(cliconn, storage_group)
    print volumes
    
    #get all the volumes from storage group and delete them
    for volume in volumes:
        delete_volume(cliconn, configService, volume)
    
    port_group = find_target_masking_group(cliconn, maskingViewName)
    print port_group
    
    ports = get_ports_from_port_group(cliconn, port_group)
    print ports
    
    initiator_group = find_initiator_masking_group(cliconn, maskingViewName)
    print initiator_group
    
    initiators = get_initators_from_initiator_group(cliconn, initiator_group)
    print initiators



       
    
    
    
    
        
    
