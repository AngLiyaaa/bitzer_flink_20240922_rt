package com.bitzer_test.bean;

import java.sql.Date;
import java.util.Objects;

public class sap_api_kpi {
    public String SerialNumber;
    public String UserName;
    public Date PostingDate;
    public String ObjectId;
    public Date StartExecutionDate;
    public String StartExecutionTime;
    public String UniqueAdditFieldID;
    public String AdditionalField;
    public String AdditionalFieldData;
    public Date AdditemDate;
    public String AdditemTime;
    public String OrderNumber;
    public String ActivityNumber;
    public String MaterialNumber;
    public Date EntryDateOfConfirmation;
    public String ConfirmationEntryTime;
    public String User_Name;
    public String SalesOrderNumber;
    public String SalesOrderItemNumber;
    public String OrderType;
    public String MaterialType;
    public String ControlKey;
    public String StorageLocation;
    public String Plant;
    public String ProductType;
    public String Family;
    public String Characteristic;

    // 无参构造函数
    public sap_api_kpi() {
    }

    // 有参构造函数（可选）
    public sap_api_kpi(String serialNumber, String userName, Date postingDate, String objectId, Date startExecutionDate, String startExecutionTime, String uniqueAdditFieldID, String additionalField, String additionalFieldData, Date additemDate, String additemTime, String orderNumber, String activityNumber, String materialNumber, Date entryDateOfConfirmation, String confirmationEntryTime, String user_Name, String salesOrderNumber, String salesOrderItemNumber, String orderType, String materialType, String controlKey, String storageLocation, String plant, String productType, String family, String characteristic) {
        SerialNumber = serialNumber;
        UserName = userName;
        PostingDate = postingDate;
        ObjectId = objectId;
        StartExecutionDate = startExecutionDate;
        StartExecutionTime = startExecutionTime;
        UniqueAdditFieldID = uniqueAdditFieldID;
        AdditionalField = additionalField;
        AdditionalFieldData = additionalFieldData;
        AdditemDate = additemDate;
        AdditemTime = additemTime;
        OrderNumber = orderNumber;
        ActivityNumber = activityNumber;
        MaterialNumber = materialNumber;
        EntryDateOfConfirmation = entryDateOfConfirmation;
        ConfirmationEntryTime = confirmationEntryTime;
        User_Name = user_Name;
        SalesOrderNumber = salesOrderNumber;
        SalesOrderItemNumber = salesOrderItemNumber;
        OrderType = orderType;
        MaterialType = materialType;
        ControlKey = controlKey;
        StorageLocation = storageLocation;
        Plant = plant;
        ProductType = productType;
        Family = family;
        Characteristic = characteristic;
    }

    public String getSerialNumber() {
        return SerialNumber;
    }

    public void setSerialNumber(String serialNumber) {
        SerialNumber = serialNumber;
    }

    public String getUserName() {
        return UserName;
    }

    public void setUserName(String userName) {
        UserName = userName;
    }

    public Date getPostingDate() {
        return PostingDate;
    }

    public void setPostingDate(Date postingDate) {
        PostingDate = postingDate;
    }

    public String getObjectId() {
        return ObjectId;
    }

    public void setObjectId(String objectId) {
        ObjectId = objectId;
    }

    public Date getStartExecutionDate() {
        return StartExecutionDate;
    }

    public void setStartExecutionDate(Date startExecutionDate) {
        StartExecutionDate = startExecutionDate;
    }

    public String getStartExecutionTime() {
        return StartExecutionTime;
    }

    public void setStartExecutionTime(String startExecutionTime) {
        StartExecutionTime = startExecutionTime;
    }

    public String getUniqueAdditFieldID() {
        return UniqueAdditFieldID;
    }

    public void setUniqueAdditFieldID(String uniqueAdditFieldID) {
        UniqueAdditFieldID = uniqueAdditFieldID;
    }

    public String getAdditionalField() {
        return AdditionalField;
    }

    public void setAdditionalField(String additionalField) {
        AdditionalField = additionalField;
    }

    public String getAdditionalFieldData() {
        return AdditionalFieldData;
    }

    public void setAdditionalFieldData(String additionalFieldData) {
        AdditionalFieldData = additionalFieldData;
    }

    public Date getAdditemDate() {
        return AdditemDate;
    }

    public void setAdditemDate(Date additemDate) {
        AdditemDate = additemDate;
    }

    public String getAdditemTime() {
        return AdditemTime;
    }

    public void setAdditemTime(String additemTime) {
        AdditemTime = additemTime;
    }

    public String getOrderNumber() {
        return OrderNumber;
    }

    public void setOrderNumber(String orderNumber) {
        OrderNumber = orderNumber;
    }

    public String getActivityNumber() {
        return ActivityNumber;
    }

    public void setActivityNumber(String activityNumber) {
        ActivityNumber = activityNumber;
    }

    public String getMaterialNumber() {
        return MaterialNumber;
    }

    public void setMaterialNumber(String materialNumber) {
        MaterialNumber = materialNumber;
    }

    public Date getEntryDateOfConfirmation() {
        return EntryDateOfConfirmation;
    }

    public void setEntryDateOfConfirmation(Date entryDateOfConfirmation) {
        EntryDateOfConfirmation = entryDateOfConfirmation;
    }

    public String getConfirmationEntryTime() {
        return ConfirmationEntryTime;
    }

    public void setConfirmationEntryTime(String confirmationEntryTime) {
        ConfirmationEntryTime = confirmationEntryTime;
    }

    public String getUser_Name() {
        return User_Name;
    }

    public void setUser_Name(String user_Name) {
        User_Name = user_Name;
    }

    public String getSalesOrderNumber() {
        return SalesOrderNumber;
    }

    public void setSalesOrderNumber(String salesOrderNumber) {
        SalesOrderNumber = salesOrderNumber;
    }

    public String getSalesOrderItemNumber() {
        return SalesOrderItemNumber;
    }

    public void setSalesOrderItemNumber(String salesOrderItemNumber) {
        SalesOrderItemNumber = salesOrderItemNumber;
    }

    public String getOrderType() {
        return OrderType;
    }

    public void setOrderType(String orderType) {
        OrderType = orderType;
    }

    public String getMaterialType() {
        return MaterialType;
    }

    public void setMaterialType(String materialType) {
        MaterialType = materialType;
    }

    public String getControlKey() {
        return ControlKey;
    }

    public void setControlKey(String controlKey) {
        ControlKey = controlKey;
    }

    public String getStorageLocation() {
        return StorageLocation;
    }

    public void setStorageLocation(String storageLocation) {
        StorageLocation = storageLocation;
    }

    public String getPlant() {
        return Plant;
    }

    public void setPlant(String plant) {
        Plant = plant;
    }

    public String getProductType() {
        return ProductType;
    }

    public void setProductType(String productType) {
        ProductType = productType;
    }

    public String getFamily() {
        return Family;
    }

    public void setFamily(String family) {
        Family = family;
    }

    public String getCharacteristic() {
        return Characteristic;
    }

    public void setCharacteristic(String characteristic) {
        Characteristic = characteristic;
    }

    @Override
    public String toString() {
        return "sap_api_kpi{" +
                "SerialNumber='" + SerialNumber + '\'' +
                ", UserName='" + UserName + '\'' +
                ", PostingDate=" + PostingDate +
                ", ObjectId='" + ObjectId + '\'' +
                ", StartExecutionDate=" + StartExecutionDate +
                ", StartExecutionTime='" + StartExecutionTime + '\'' +
                ", UniqueAdditFieldID='" + UniqueAdditFieldID + '\'' +
                ", AdditionalField='" + AdditionalField + '\'' +
                ", AdditionalFieldData='" + AdditionalFieldData + '\'' +
                ", AdditemDate=" + AdditemDate +
                ", AdditemTime='" + AdditemTime + '\'' +
                ", OrderNumber='" + OrderNumber + '\'' +
                ", ActivityNumber='" + ActivityNumber + '\'' +
                ", MaterialNumber='" + MaterialNumber + '\'' +
                ", EntryDateOfConfirmation=" + EntryDateOfConfirmation +
                ", ConfirmationEntryTime='" + ConfirmationEntryTime + '\'' +
                ", User_Name='" + User_Name + '\'' +
                ", SalesOrderNumber='" + SalesOrderNumber + '\'' +
                ", SalesOrderItemNumber='" + SalesOrderItemNumber + '\'' +
                ", OrderType='" + OrderType + '\'' +
                ", MaterialType='" + MaterialType + '\'' +
                ", ControlKey='" + ControlKey + '\'' +
                ", StorageLocation='" + StorageLocation + '\'' +
                ", Plant='" + Plant + '\'' +
                ", ProductType='" + ProductType + '\'' +
                ", Family='" + Family + '\'' +
                ", Characteristic='" + Characteristic + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        sap_api_kpi sapApiKpi = (sap_api_kpi) o;
        return Objects.equals(SerialNumber, sapApiKpi.SerialNumber) && Objects.equals(UserName, sapApiKpi.UserName) && Objects.equals(PostingDate, sapApiKpi.PostingDate) && Objects.equals(ObjectId, sapApiKpi.ObjectId) && Objects.equals(StartExecutionDate, sapApiKpi.StartExecutionDate) && Objects.equals(StartExecutionTime, sapApiKpi.StartExecutionTime) && Objects.equals(UniqueAdditFieldID, sapApiKpi.UniqueAdditFieldID) && Objects.equals(AdditionalField, sapApiKpi.AdditionalField) && Objects.equals(AdditionalFieldData, sapApiKpi.AdditionalFieldData) && Objects.equals(AdditemDate, sapApiKpi.AdditemDate) && Objects.equals(AdditemTime, sapApiKpi.AdditemTime) && Objects.equals(OrderNumber, sapApiKpi.OrderNumber) && Objects.equals(ActivityNumber, sapApiKpi.ActivityNumber) && Objects.equals(MaterialNumber, sapApiKpi.MaterialNumber) && Objects.equals(EntryDateOfConfirmation, sapApiKpi.EntryDateOfConfirmation) && Objects.equals(ConfirmationEntryTime, sapApiKpi.ConfirmationEntryTime) && Objects.equals(User_Name, sapApiKpi.User_Name) && Objects.equals(SalesOrderNumber, sapApiKpi.SalesOrderNumber) && Objects.equals(SalesOrderItemNumber, sapApiKpi.SalesOrderItemNumber) && Objects.equals(OrderType, sapApiKpi.OrderType) && Objects.equals(MaterialType, sapApiKpi.MaterialType) && Objects.equals(ControlKey, sapApiKpi.ControlKey) && Objects.equals(StorageLocation, sapApiKpi.StorageLocation) && Objects.equals(Plant, sapApiKpi.Plant) && Objects.equals(ProductType, sapApiKpi.ProductType) && Objects.equals(Family, sapApiKpi.Family) && Objects.equals(Characteristic, sapApiKpi.Characteristic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(SerialNumber, UserName, PostingDate, ObjectId, StartExecutionDate, StartExecutionTime, UniqueAdditFieldID, AdditionalField, AdditionalFieldData, AdditemDate, AdditemTime, OrderNumber, ActivityNumber, MaterialNumber, EntryDateOfConfirmation, ConfirmationEntryTime, User_Name, SalesOrderNumber, SalesOrderItemNumber, OrderType, MaterialType, ControlKey, StorageLocation, Plant, ProductType, Family, Characteristic);
    }
}
