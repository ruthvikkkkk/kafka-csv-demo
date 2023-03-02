package com.example.repository;

import com.example.entities.FileEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface FileRepository extends JpaRepository<FileEntity, String> {

    @Modifying
    @Query("update FileEntity f set status = 'finished uploading!' where file_name = :name")
    void updateStatus(@Param("name") String name);
}
