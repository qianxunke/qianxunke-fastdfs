package model

import (
	"errors"
	"net/http"
	"qianxunke-fastdfs/common/api"
)

func (this *dao) InsertFileInfo(fileInfo *FileInfo) (err error) {
	defer func() {
		if re := recover(); re != nil {
			if err == nil {
				err = errors.New("[file model InsertFileInfo] error")
			}
		}
	}()
	db := GetDB()
	//判断是否有重复文件
	var tmpFile FileInfo
	db.Model(&FileInfo{}).Where("name = ? and md5 = ?", fileInfo.Name, fileInfo.Md5).First(&tmpFile)
	if tmpFile.ID > 0 {
		db.Model(&FileInfo{}).Update(&fileInfo)
	}else {
		err = db.Model(&FileInfo{}).Save(&fileInfo).Error
	}
	return
}

func (this *dao) UpdateFileInfo(fileInfo *FileInfo) (err error) {
	defer func() {
		if re := recover(); re != nil {
			if err == nil {
				err = errors.New("[file model UpdateFileInfo] error")
			}
		}
	}()
	db := GetDB()
	//判断是否有重复文件
	var tmpFile FileInfo
	db.Model(&FileInfo{}).Where("name = ? and md5 = ?", fileInfo.Name, fileInfo.Md5).First(&tmpFile)
	if tmpFile.ID > 0 {
		db.Model(&FileInfo{}).Update(&fileInfo)
	}else {
		err = db.Model(&FileInfo{}).Save(&fileInfo).Error
	}
	return
}

func (this *dao) DeleteFileInfoById(id uint) (err error) {
	defer func() {
		if re := recover(); re != nil {
			if err == nil {
				err = errors.New("[DeleteFileInfoById] error")
			}
		}
	}()

	DB := GetDB()
	return DB.Model(&FileInfo{}).Where(" id = ?", id).Error

}

func (this *dao) SearchFileInfoList(limit int64, pages int64, key string, startTime string, endTime string, order string) (data *api.ListResponseEntity, err error) {
	defer func() {
		if re := recover(); re != nil {
			if err == nil {
				err = errors.New("[SearchFileInfoList] error")
			}
		}
	}()
	DB := GetDB()
	data = &api.ListResponseEntity{}
	var list *[]FileInfo
	offset := (pages - 1) * limit
	if len(key) == 0 {
		if len(startTime) > 0 && len(endTime) == 0 {
			err = DB.Model(&FileInfo{}).Where("created_time > ?", endTime).Order(order).Count(data.Total).Error
			if err == nil && data.Total > 0 {
				err = DB.Where("created_time > ? ", startTime).Order(order).Offset(offset).Limit(limit).Find(&list).Error
			}
		} else if len(startTime) == 0 && len(endTime) > 0 {
			err = DB.Model(&FileInfo{}).Where("created_time < ? ", endTime).Order(order).Count(data.Total).Error
			if err == nil && data.Total > 0 {
				err = DB.Where("created_time < ? ", endTime).Order(order).Offset(offset).Limit(limit).Find(&list).Error
			}
		} else if len(startTime) > 0 && len(endTime) > 0 {
			err = DB.Model(&FileInfo{}).Where("created_time  between ? and ?", startTime, endTime).Order(order).Count(data.Total).Error
			if err == nil && data.Total > 0 {
				err = DB.Where("created_time  between ? and ?", startTime, endTime).Order(order).Offset(offset).Limit(limit).Find(&list).Error
			}
		} else {
			//先统计
			err = DB.Model(&FileInfo{}).Order(order).Count(data.Total).Error
			if err == nil && data.Total > 0 {
				err = DB.Order(order).Offset(offset).Limit(limit).Find(&list).Error
			}
		}
	} else {
		searchKey := "%" + key + "%"
		if len(startTime) > 0 && len(endTime) == 0 {
			err = DB.Model(&FileInfo{}).Where("(name like ? ) and created_time > ? ", searchKey, startTime).Order(order).Count(&list).Error
			if err == nil && data.Total > 0 {
				err = DB.Model(&FileInfo{}).Where("(name like ?) and created_time > ? ", searchKey, startTime).Order(order).Offset(offset).Limit(limit).Find(&list).Error
			}
		} else if len(startTime) == 0 && len(endTime) > 0 {
			err = DB.Model(&FileInfo{}).Where("(name like ?) and created_time < ? ", searchKey, endTime).Order(order).Count(&data.Total).Error
			if err == nil && data.Total > 0 {
				err = DB.Where("(name like ?) and created_time < ? ", searchKey, endTime).Order(order).Offset(offset).Limit(limit).Find(&list).Error
			}
		} else if len(startTime) > 0 && len(endTime) > 0 {
			err = DB.Model(&FileInfo{}).Where("(name like ?) and created_time between ? and ?", searchKey, startTime, endTime).Order(order).Count(&data.Total).Error
			if err == nil && data.Total > 0 {
				err = DB.Where("(name like ?) and created_time between ? and ?", searchKey, startTime, endTime).Order(order).Offset(offset).Limit(limit).Find(&list).Error
			}
		} else {
			err = DB.Model(&FileInfo{}).Where("name like ?", searchKey).Order(order).Count(&data.Total).Error
			if err == nil && data.Total > 0 {
				err = DB.Where("name like ?", searchKey).Order(order).Offset(offset).Limit(limit).Find(&list).Error
			}
		}
	}
	if err != nil {
		if data.Total > 0 {
			data.Data = &list
		}
		data.Code = http.StatusOK
		data.Page = pages
		data.Limit = limit
		data.Message = "ok"
	}
	return
}



