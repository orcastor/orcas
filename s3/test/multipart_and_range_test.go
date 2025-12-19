package s3_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/s3"
	"github.com/orcastor/orcas/s3/util"
)

// setupTestEnvironment 设置测试环境
func setupTestEnvironmentForMultipart(t *testing.T) (int64, *gin.Engine) {
	// 为每个测试创建独立的临时目录（使用短名称避免路径过长）
	testID := time.Now().UnixNano() % 1000000000 // 只取后9位数字，缩短路径
	baseDir := filepath.Join(os.TempDir(), fmt.Sprintf("o_s3_%d", testID))
	dataDir := filepath.Join(os.TempDir(), fmt.Sprintf("o_s3d_%d", testID))

	os.MkdirAll(baseDir, 0o755)
	os.MkdirAll(dataDir, 0o755)

	// 设置环境变量
	os.Setenv("ORCAS_BASE", baseDir)
	os.Setenv("ORCAS_DATA", dataDir)
	// 禁用批量写入以避免测试中的时序问题
	os.Setenv("ORCAS_BATCH_WRITE_ENABLED", "false")
	core.ORCAS_BASE = baseDir
	core.ORCAS_DATA = dataDir

	// 初始化数据库（必须在设置环境变量之后）
	// 注意：core.InitDB()会检查数据库是否已初始化，如果已初始化则不会重新初始化
	// 为了确保每个测试使用独立的数据库，我们需要先关闭旧连接
	// 但由于core.InitDB()的实现，我们需要确保环境变量已设置
	core.InitDB("")

	// 等待一小段时间确保数据库连接稳定
	time.Sleep(50 * time.Millisecond)

	ensureTestUserForMultipart(t)

	// 创建测试bucket
	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	err := core.InitBucketDB(context.Background(), testBktID)
	if err != nil {
		t.Fatalf("InitBucketDB failed: %v", err)
	}

	// 登录并创建bucket
	handler := core.NewLocalHandler()
	ctx, _, _, err := handler.Login(context.Background(), "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	admin := core.NewLocalAdmin()
	bkt := &core.BucketInfo{
		ID:        testBktID,
		Name:      "test-bucket",
		UID:       1,
		Type:      1,
		Quota:     -1,              // 无限制配额
		ChunkSize: 4 * 1024 * 1024, // 4MB chunk size for testing multi-chunk reads
	}
	err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
	if err != nil {
		t.Fatalf("PutBkt failed: %v", err)
	}

	// 等待bucket信息写入数据库，确保后续查询可以成功
	// PutData内部会调用GetBkt来检查配额，需要确保bucket信息已经写入数据库
	time.Sleep(200 * time.Millisecond)

	// 设置gin为测试模式
	gin.SetMode(gin.TestMode)

	// 创建测试用的gin engine
	router := gin.New()
	router.Use(func(c *gin.Context) {
		// 模拟JWT认证，设置UID
		c.Set("uid", int64(1))
		c.Request = c.Request.WithContext(core.UserInfo2Ctx(c.Request.Context(), &core.UserInfo{
			ID: 1,
		}))
		c.Next()
	})

	// 注册路由
	router.GET("/", s3.ListBuckets)
	router.PUT("/:bucket", s3.CreateBucket)
	router.DELETE("/:bucket", s3.DeleteBucket)
	// 分片上传和普通操作的路由（通过查询参数区分）
	router.GET("/:bucket", func(c *gin.Context) {
		_, hasUploads := c.GetQuery("uploads")
		if hasUploads {
			s3.ListMultipartUploads(c)
		} else {
			s3.ListObjects(c)
		}
	})
	router.GET("/:bucket/*key", func(c *gin.Context) {
		if c.Query("uploadId") != "" && c.Request.Method == "GET" {
			s3.ListParts(c)
		} else {
			s3.GetObject(c)
		}
	})
	router.PUT("/:bucket/*key", func(c *gin.Context) {
		if c.Query("partNumber") != "" && c.Query("uploadId") != "" {
			s3.UploadPart(c)
		} else {
			s3.PutObject(c)
		}
	})
	router.POST("/:bucket/*key", func(c *gin.Context) {
		_, hasUploads := c.GetQuery("uploads")
		uploadId := c.Query("uploadId")
		if hasUploads {
			s3.InitiateMultipartUpload(c)
		} else if uploadId != "" {
			s3.CompleteMultipartUpload(c)
		} else {
			util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "Missing required query parameter (uploads or uploadId)")
		}
	})
	router.DELETE("/:bucket/*key", func(c *gin.Context) {
		if c.Query("uploadId") != "" {
			s3.AbortMultipartUpload(c)
		} else {
			s3.DeleteObject(c)
		}
	})
	router.HEAD("/:bucket/*key", s3.HeadObject)

	return testBktID, router
}

// ensureTestUserForMultipart 确保测试用户存在
func ensureTestUserForMultipart(t *testing.T) {
	handler := core.NewLocalHandler()
	ctx := context.Background()
	_, _, _, err := handler.Login(ctx, "orcas", "orcas")
	if err == nil {
		return
	}

	// 如果登录失败，尝试创建用户
	hashedPwd := "1000:Zd54dfEjoftaY8NiAINGag==:q1yB510yT5tGIGNewItVSg=="
	db, err := core.GetDB()
	if err != nil {
		t.Logf("Warning: Failed to get DB: %v", err)
		return
	}
	defer db.Close()

	_, err = db.Exec(`INSERT OR IGNORE INTO usr (id, role, usr, pwd, name, avatar, key) VALUES (1, 1, 'orcas', ?, 'orcas', '', '')`, hashedPwd)
	if err != nil {
		t.Logf("Warning: Failed to create user: %v", err)
	}
}

// generateLargeTestData 生成大文件测试数据（超过chunk size）
func generateLargeTestData(size int64) []byte {
	data := make([]byte, size)
	for i := int64(0); i < size; i++ {
		data[i] = byte(i % 256)
	}
	return data
}

// waitForObjectExists 等待对象存在并可访问
func waitForObjectExists(t *testing.T, router *gin.Engine, bucketName, key string, maxRetries int) bool {
	for i := 0; i < maxRetries; i++ {
		req := httptest.NewRequest("HEAD", fmt.Sprintf("/%s/%s", bucketName, key), nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code == http.StatusOK {
			// 对于大文件，尝试读取一小部分数据来验证数据块是否真的存在
			req2 := httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, key), nil)
			req2.Header.Set("Range", "bytes=0-0") // 只读取第一个字节
			w2 := httptest.NewRecorder()
			router.ServeHTTP(w2, req2)
			if w2.Code == http.StatusOK || w2.Code == http.StatusPartialContent {
				return true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// ==================== 随机读涉及多块的测试用例 ====================

// TestRangeReadSingleChunk 测试单块范围内的随机读
func TestRangeReadSingleChunk(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"
	key := "test-large-file"

	// 创建一个大文件（10MB，超过默认4MB chunk size）
	chunkSize := int64(4 * 1024 * 1024) // 4MB
	fileSize := chunkSize * 2           // 8MB，跨越2个chunk
	testData := generateLargeTestData(fileSize)

	// 1. 上传文件
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PutObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	// 等待数据写入完成并验证文件存在
	// 先简单等待一下，然后检查文件是否存在
	time.Sleep(200 * time.Millisecond)
	if !waitForObjectExists(t, router, bucketName, key, 30) {
		// 如果文件不存在，尝试直接读取看看错误信息
		req2 := httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, key), nil)
		w2 := httptest.NewRecorder()
		router.ServeHTTP(w2, req2)
		t.Fatalf("File not found after upload: GET status=%d, body=%s", w2.Code, w2.Body.String())
	}

	// 额外等待以确保数据块完全写入（大文件需要更多时间）
	time.Sleep(500 * time.Millisecond)

	// 2. 测试单块范围内的随机读（第一个chunk内）
	offset := int64(1 * 1024 * 1024) // 1MB offset
	size := int64(2 * 1024 * 1024)   // 2MB size
	req = httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, key), nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+size-1))
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusPartialContent {
		t.Fatalf("Range read failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	expectedData := testData[offset : offset+size]
	if !bytes.Equal(w.Body.Bytes(), expectedData) {
		t.Errorf("Range read data mismatch: expected %d bytes, got %d bytes", len(expectedData), len(w.Body.Bytes()))
	}

	// 验证Content-Range header
	contentRange := w.Header().Get("Content-Range")
	expectedRange := fmt.Sprintf("bytes %d-%d/%d", offset, offset+size-1, fileSize)
	if !strings.Contains(contentRange, expectedRange) {
		t.Errorf("Content-Range header mismatch: expected contains %s, got %s", expectedRange, contentRange)
	}
}

// TestRangeReadMultipleChunks 测试跨多个块的随机读
func TestRangeReadMultipleChunks(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"
	key := "test-multi-chunk-file"

	// 创建一个大文件（12MB，跨越3个chunk）
	chunkSize := int64(4 * 1024 * 1024) // 4MB
	fileSize := chunkSize * 3           // 12MB
	testData := generateLargeTestData(fileSize)

	// 1. 上传文件
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PutObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	// 等待数据写入完成
	if !waitForObjectExists(t, router, bucketName, key, 10) {
		t.Fatalf("File not found after upload")
	}

	// 2. 测试跨多个块的随机读（从第一个chunk中间到第三个chunk中间）
	offset := int64(2 * 1024 * 1024) // 2MB offset（第一个chunk中间）
	size := int64(6 * 1024 * 1024)   // 6MB size（跨越3个chunk）
	req = httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, key), nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+size-1))
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusPartialContent {
		t.Fatalf("Range read failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	expectedData := testData[offset : offset+size]
	if !bytes.Equal(w.Body.Bytes(), expectedData) {
		t.Errorf("Range read data mismatch: expected %d bytes, got %d bytes", len(expectedData), len(w.Body.Bytes()))
		// 检查前几个字节
		if len(w.Body.Bytes()) > 0 && len(expectedData) > 0 {
			t.Errorf("First byte: expected %d, got %d", expectedData[0], w.Body.Bytes()[0])
		}
	}
}

// TestRangeReadFromStartToMiddle 测试从文件开始到中间位置的随机读
func TestRangeReadFromStartToMiddle(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"
	key := "test-start-to-middle"

	chunkSize := int64(4 * 1024 * 1024) // 4MB
	fileSize := chunkSize * 2           // 8MB
	testData := generateLargeTestData(fileSize)

	// 上传文件
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PutObject failed: status=%d", w.Code)
	}

	// 等待数据写入完成
	if !waitForObjectExists(t, router, bucketName, key, 10) {
		t.Fatalf("File not found after upload")
	}

	// 从开始读到第一个chunk结束
	offset := int64(0)
	size := chunkSize
	req = httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, key), nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+size-1))
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusPartialContent {
		t.Fatalf("Range read failed: status=%d", w.Code)
	}

	expectedData := testData[offset : offset+size]
	if !bytes.Equal(w.Body.Bytes(), expectedData) {
		t.Errorf("Range read data mismatch")
	}
}

// TestRangeReadFromMiddleToEnd 测试从中间位置到文件末尾的随机读
func TestRangeReadFromMiddleToEnd(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"
	key := "test-middle-to-end"

	chunkSize := int64(4 * 1024 * 1024) // 4MB
	fileSize := chunkSize*2 + 1024*1024 // 9MB（最后一个chunk不完整）
	testData := generateLargeTestData(fileSize)

	// 上传文件
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PutObject failed: status=%d", w.Code)
	}

	// 等待数据写入完成
	if !waitForObjectExists(t, router, bucketName, key, 10) {
		t.Fatalf("File not found after upload")
	}

	// 从第二个chunk中间读到文件末尾
	offset := chunkSize + int64(2*1024*1024) // 6MB offset
	size := fileSize - offset                // 读到末尾
	req = httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, key), nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+size-1))
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusPartialContent {
		t.Fatalf("Range read failed: status=%d", w.Code)
	}

	expectedData := testData[offset : offset+size]
	if !bytes.Equal(w.Body.Bytes(), expectedData) {
		t.Errorf("Range read data mismatch: expected %d bytes, got %d bytes", len(expectedData), len(w.Body.Bytes()))
	}
}

// TestRangeReadSuffix 测试后缀范围读取（从指定位置到文件末尾）
func TestRangeReadSuffix(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"
	key := "test-suffix-read"

	chunkSize := int64(4 * 1024 * 1024) // 4MB
	fileSize := chunkSize * 3           // 12MB
	testData := generateLargeTestData(fileSize)

	// 上传文件
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PutObject failed: status=%d", w.Code)
	}

	// 等待数据写入完成
	if !waitForObjectExists(t, router, bucketName, key, 10) {
		t.Fatalf("File not found after upload")
	}

	// 使用后缀范围（从倒数5MB开始）
	suffixSize := int64(5 * 1024 * 1024)
	req = httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, key), nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=-%d", suffixSize))
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusPartialContent {
		t.Fatalf("Suffix range read failed: status=%d", w.Code)
	}

	expectedData := testData[fileSize-suffixSize:]
	if !bytes.Equal(w.Body.Bytes(), expectedData) {
		t.Errorf("Suffix range read data mismatch: expected %d bytes, got %d bytes", len(expectedData), len(w.Body.Bytes()))
	}
}

// TestRangeReadMultipleChunksLarge 测试跨多个大块的随机读
func TestRangeReadMultipleChunksLarge(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"
	key := "test-large-multi-chunk"

	// 创建更大的文件（20MB，跨越5个chunk）
	chunkSize := int64(4 * 1024 * 1024) // 4MB
	fileSize := chunkSize * 5           // 20MB
	testData := generateLargeTestData(fileSize)

	// 上传文件
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PutObject failed: status=%d", w.Code)
	}

	// 等待数据写入完成
	if !waitForObjectExists(t, router, bucketName, key, 10) {
		t.Fatalf("File not found after upload")
	}

	// 测试跨多个块的随机读（跨越所有5个chunk）
	offset := int64(1 * 1024 * 1024) // 1MB offset
	size := int64(18 * 1024 * 1024)  // 18MB size（跨越所有chunk）
	req = httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, key), nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+size-1))
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusPartialContent {
		t.Fatalf("Range read failed: status=%d", w.Code)
	}

	expectedData := testData[offset : offset+size]
	if !bytes.Equal(w.Body.Bytes(), expectedData) {
		t.Errorf("Range read data mismatch: expected %d bytes, got %d bytes", len(expectedData), len(w.Body.Bytes()))
	}
}

// ==================== 分片上传的测试用例 ====================

// TestMultipartUploadBasic 测试基本的分片上传流程
func TestMultipartUploadBasic(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"
	key := "test-multipart-file"

	// 准备分片数据
	part1Data := []byte("part 1 data")
	part2Data := []byte("part 2 data")
	part3Data := []byte("part 3 data")
	expectedData := append(append(part1Data, part2Data...), part3Data...)

	// 1. 初始化分片上传
	url := fmt.Sprintf("/%s/%s?uploads", bucketName, key)
	req := httptest.NewRequest("POST", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("InitiateMultipartUpload failed: status=%d, body=%s, url=%s", w.Code, w.Body.String(), url)
	}

	var initResult struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		UploadID string   `xml:"UploadId"`
	}
	if err := xml.Unmarshal(w.Body.Bytes(), &initResult); err != nil {
		t.Fatalf("Failed to unmarshal InitiateMultipartUploadResult: %v", err)
	}
	if initResult.UploadID == "" {
		t.Fatal("UploadID is empty")
	}
	uploadID := initResult.UploadID

	// 2. 上传分片
	parts := []struct {
		partNumber int
		data       []byte
		etag       string
	}{
		{1, part1Data, ""},
		{2, part2Data, ""},
		{3, part3Data, ""},
	}

	for i, part := range parts {
		req = httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s?partNumber=%d&uploadId=%s", bucketName, key, part.partNumber, uploadID), bytes.NewReader(part.data))
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("UploadPart %d failed: status=%d, body=%s", part.partNumber, w.Code, w.Body.String())
		}
		parts[i].etag = w.Header().Get("ETag")
		if parts[i].etag == "" {
			t.Fatalf("ETag is empty for part %d", part.partNumber)
		}
	}

	// 3. 完成分片上传
	completeXML := fmt.Sprintf(`<CompleteMultipartUpload>
		<Part>
			<PartNumber>1</PartNumber>
			<ETag>%s</ETag>
		</Part>
		<Part>
			<PartNumber>2</PartNumber>
			<ETag>%s</ETag>
		</Part>
		<Part>
			<PartNumber>3</PartNumber>
			<ETag>%s</ETag>
		</Part>
	</CompleteMultipartUpload>`, parts[0].etag, parts[1].etag, parts[2].etag)

	req = httptest.NewRequest("POST", fmt.Sprintf("/%s/%s?uploadId=%s", bucketName, key, uploadID), bytes.NewReader([]byte(completeXML)))
	req.Header.Set("Content-Type", "application/xml")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("CompleteMultipartUpload failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	// 4. 等待文件写入完成并验证上传的文件
	if !waitForObjectExists(t, router, bucketName, key, 10) {
		t.Fatalf("File not found after multipart upload completion")
	}

	req = httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, key), nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GetObject failed: status=%d", w.Code)
	}

	if !bytes.Equal(w.Body.Bytes(), expectedData) {
		t.Errorf("Uploaded file data mismatch: expected %d bytes, got %d bytes", len(expectedData), len(w.Body.Bytes()))
	}
}

// TestMultipartUploadLargeParts 测试大分片上传
func TestMultipartUploadLargeParts(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"
	key := "test-large-multipart"

	// 准备大分片数据（每个分片5MB）
	part1Data := generateLargeTestData(5 * 1024 * 1024)
	part2Data := generateLargeTestData(5 * 1024 * 1024)
	expectedData := append(part1Data, part2Data...)

	// 1. 初始化分片上传
	req := httptest.NewRequest("POST", fmt.Sprintf("/%s/%s?uploads", bucketName, key), nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("InitiateMultipartUpload failed: status=%d", w.Code)
	}

	var initResult struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		UploadID string   `xml:"UploadId"`
	}
	xml.Unmarshal(w.Body.Bytes(), &initResult)
	uploadID := initResult.UploadID

	// 2. 上传大分片
	parts := []struct {
		partNumber int
		data       []byte
		etag       string
	}{
		{1, part1Data, ""},
		{2, part2Data, ""},
	}

	for i, part := range parts {
		req = httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s?partNumber=%d&uploadId=%s", bucketName, key, part.partNumber, uploadID), bytes.NewReader(part.data))
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("UploadPart %d failed: status=%d", part.partNumber, w.Code)
		}
		parts[i].etag = w.Header().Get("ETag")
	}

	// 3. 完成分片上传
	completeXML := fmt.Sprintf(`<CompleteMultipartUpload>
		<Part>
			<PartNumber>1</PartNumber>
			<ETag>%s</ETag>
		</Part>
		<Part>
			<PartNumber>2</PartNumber>
			<ETag>%s</ETag>
		</Part>
	</CompleteMultipartUpload>`, parts[0].etag, parts[1].etag)

	req = httptest.NewRequest("POST", fmt.Sprintf("/%s/%s?uploadId=%s", bucketName, key, uploadID), bytes.NewReader([]byte(completeXML)))
	req.Header.Set("Content-Type", "application/xml")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("CompleteMultipartUpload failed: status=%d", w.Code)
	}

	// 4. 等待文件写入完成并验证上传的文件
	if !waitForObjectExists(t, router, bucketName, key, 10) {
		t.Fatalf("File not found after multipart upload completion")
	}

	req = httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, key), nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GetObject failed: status=%d", w.Code)
	}

	if !bytes.Equal(w.Body.Bytes(), expectedData) {
		t.Errorf("Uploaded file data mismatch: expected %d bytes, got %d bytes", len(expectedData), len(w.Body.Bytes()))
	}
}

// TestMultipartUploadManyParts 测试多个分片上传
func TestMultipartUploadManyParts(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"
	key := "test-many-parts"

	// 准备10个分片
	numParts := 10
	partsData := make([][]byte, numParts)
	var expectedData []byte
	for i := 0; i < numParts; i++ {
		partsData[i] = []byte(fmt.Sprintf("part %d data", i+1))
		expectedData = append(expectedData, partsData[i]...)
	}

	// 1. 初始化分片上传
	req := httptest.NewRequest("POST", fmt.Sprintf("/%s/%s?uploads", bucketName, key), nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("InitiateMultipartUpload failed: status=%d", w.Code)
	}

	var initResult struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		UploadID string   `xml:"UploadId"`
	}
	xml.Unmarshal(w.Body.Bytes(), &initResult)
	uploadID := initResult.UploadID

	// 2. 上传所有分片
	etags := make([]string, numParts)
	for i := 0; i < numParts; i++ {
		req = httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s?partNumber=%d&uploadId=%s", bucketName, key, i+1, uploadID), bytes.NewReader(partsData[i]))
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("UploadPart %d failed: status=%d", i+1, w.Code)
		}
		etags[i] = w.Header().Get("ETag")
	}

	// 3. 构建完成请求XML
	var completeParts strings.Builder
	completeParts.WriteString("<CompleteMultipartUpload>")
	for i := 0; i < numParts; i++ {
		completeParts.WriteString(fmt.Sprintf(`<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>`, i+1, etags[i]))
	}
	completeParts.WriteString("</CompleteMultipartUpload>")

	req = httptest.NewRequest("POST", fmt.Sprintf("/%s/%s?uploadId=%s", bucketName, key, uploadID), bytes.NewReader([]byte(completeParts.String())))
	req.Header.Set("Content-Type", "application/xml")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("CompleteMultipartUpload failed: status=%d", w.Code)
	}

	// 4. 等待文件写入完成并验证上传的文件
	if !waitForObjectExists(t, router, bucketName, key, 10) {
		t.Fatalf("File not found after multipart upload completion")
	}

	req = httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, key), nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GetObject failed: status=%d", w.Code)
	}

	if !bytes.Equal(w.Body.Bytes(), expectedData) {
		t.Errorf("Uploaded file data mismatch: expected %d bytes, got %d bytes", len(expectedData), len(w.Body.Bytes()))
	}
}

// TestMultipartUploadListParts 测试列出分片
func TestMultipartUploadListParts(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"
	key := "test-list-parts"

	// 1. 初始化分片上传
	req := httptest.NewRequest("POST", fmt.Sprintf("/%s/%s?uploads", bucketName, key), nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("InitiateMultipartUpload failed: status=%d", w.Code)
	}

	var initResult struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		UploadID string   `xml:"UploadId"`
	}
	xml.Unmarshal(w.Body.Bytes(), &initResult)
	uploadID := initResult.UploadID

	// 2. 上传几个分片
	parts := []struct {
		partNumber int
		data       []byte
	}{
		{1, []byte("part 1")},
		{2, []byte("part 2")},
		{3, []byte("part 3")},
	}

	for _, part := range parts {
		req = httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s?partNumber=%d&uploadId=%s", bucketName, key, part.partNumber, uploadID), bytes.NewReader(part.data))
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("UploadPart %d failed: status=%d", part.partNumber, w.Code)
		}
	}

	// 3. 列出分片
	req = httptest.NewRequest("GET", fmt.Sprintf("/%s/%s?uploadId=%s", bucketName, key, uploadID), nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("ListParts failed: status=%d", w.Code)
	}

	var listResult struct {
		XMLName xml.Name `xml:"ListPartsResult"`
		Parts   []struct {
			PartNumber int    `xml:"PartNumber"`
			ETag       string `xml:"ETag"`
			Size       int64  `xml:"Size"`
		} `xml:"Part"`
	}
	if err := xml.Unmarshal(w.Body.Bytes(), &listResult); err != nil {
		t.Fatalf("Failed to unmarshal ListPartsResult: %v", err)
	}

	if len(listResult.Parts) != len(parts) {
		t.Errorf("Expected %d parts, got %d", len(parts), len(listResult.Parts))
	}

	for i, part := range parts {
		if i >= len(listResult.Parts) {
			t.Errorf("Part %d not found in list", part.partNumber)
			continue
		}
		if listResult.Parts[i].PartNumber != part.partNumber {
			t.Errorf("Part number mismatch: expected %d, got %d", part.partNumber, listResult.Parts[i].PartNumber)
		}
		if listResult.Parts[i].Size != int64(len(part.data)) {
			t.Errorf("Part size mismatch: expected %d, got %d", len(part.data), listResult.Parts[i].Size)
		}
	}
}

// TestMultipartUploadAbort 测试取消分片上传
func TestMultipartUploadAbort(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"
	key := "test-abort-upload"

	// 1. 初始化分片上传
	req := httptest.NewRequest("POST", fmt.Sprintf("/%s/%s?uploads", bucketName, key), nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("InitiateMultipartUpload failed: status=%d", w.Code)
	}

	var initResult struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		UploadID string   `xml:"UploadId"`
	}
	xml.Unmarshal(w.Body.Bytes(), &initResult)
	uploadID := initResult.UploadID

	// 2. 上传一个分片
	req = httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s?partNumber=1&uploadId=%s", bucketName, key, uploadID), bytes.NewReader([]byte("part 1")))
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("UploadPart failed: status=%d", w.Code)
	}

	// 3. 取消分片上传
	req = httptest.NewRequest("DELETE", fmt.Sprintf("/%s/%s?uploadId=%s", bucketName, key, uploadID), nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("AbortMultipartUpload failed: status=%d", w.Code)
	}

	// 4. 验证分片上传已被删除（尝试完成应该失败）
	completeXML := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>test</ETag></Part></CompleteMultipartUpload>`
	req = httptest.NewRequest("POST", fmt.Sprintf("/%s/%s?uploadId=%s", bucketName, key, uploadID), bytes.NewReader([]byte(completeXML)))
	req.Header.Set("Content-Type", "application/xml")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("Expected 404 after abort, got %d", w.Code)
	}
}

// TestMultipartUploadListUploads 测试列出分片上传
func TestMultipartUploadListUploads(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"

	// 1. 创建多个分片上传
	keys := []string{"test-upload-1", "test-upload-2", "test-upload-3"}
	uploadIDs := make([]string, len(keys))

	for i, key := range keys {
		req := httptest.NewRequest("POST", fmt.Sprintf("/%s/%s?uploads", bucketName, key), nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("InitiateMultipartUpload %s failed: status=%d", key, w.Code)
		}

		var initResult struct {
			XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
			UploadID string   `xml:"UploadId"`
		}
		xml.Unmarshal(w.Body.Bytes(), &initResult)
		uploadIDs[i] = initResult.UploadID
	}

	// 2. 列出分片上传
	req := httptest.NewRequest("GET", fmt.Sprintf("/%s?uploads", bucketName), nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("ListMultipartUploads failed: status=%d", w.Code)
	}

	var listResult struct {
		XMLName xml.Name `xml:"ListMultipartUploadsResult"`
		Uploads []struct {
			Key      string `xml:"Key"`
			UploadID string `xml:"UploadId"`
		} `xml:"Upload"`
	}
	if err := xml.Unmarshal(w.Body.Bytes(), &listResult); err != nil {
		t.Fatalf("Failed to unmarshal ListMultipartUploadsResult: %v", err)
	}

	if len(listResult.Uploads) < len(keys) {
		t.Errorf("Expected at least %d uploads, got %d", len(keys), len(listResult.Uploads))
	}

	// 验证所有上传都存在
	uploadMap := make(map[string]bool)
	for _, upload := range listResult.Uploads {
		uploadMap[upload.Key] = true
	}
	for _, key := range keys {
		if !uploadMap[key] {
			t.Errorf("Upload %s not found in list", key)
		}
	}
}

// TestMultipartUploadReplacePart 测试替换已存在的分片
func TestMultipartUploadReplacePart(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"
	key := "test-replace-part"

	// 1. 初始化分片上传
	req := httptest.NewRequest("POST", fmt.Sprintf("/%s/%s?uploads", bucketName, key), nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("InitiateMultipartUpload failed: status=%d", w.Code)
	}

	var initResult struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		UploadID string   `xml:"UploadId"`
	}
	xml.Unmarshal(w.Body.Bytes(), &initResult)
	uploadID := initResult.UploadID

	// 2. 上传第一个分片
	part1Data1 := []byte("original part 1")
	req = httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s?partNumber=1&uploadId=%s", bucketName, key, uploadID), bytes.NewReader(part1Data1))
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("UploadPart 1 failed: status=%d", w.Code)
	}
	etag1 := w.Header().Get("ETag")

	// 3. 替换第一个分片
	part1Data2 := []byte("replaced part 1")
	req = httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s?partNumber=1&uploadId=%s", bucketName, key, uploadID), bytes.NewReader(part1Data2))
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("ReplacePart 1 failed: status=%d", w.Code)
	}
	etag2 := w.Header().Get("ETag")

	if etag1 == etag2 {
		t.Error("ETag should be different after replacing part")
	}

	// 4. 上传第二个分片并完成
	part2Data := []byte("part 2")
	req = httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s?partNumber=2&uploadId=%s", bucketName, key, uploadID), bytes.NewReader(part2Data))
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("UploadPart 2 failed: status=%d", w.Code)
	}
	etag3 := w.Header().Get("ETag")

	completeXML := fmt.Sprintf(`<CompleteMultipartUpload>
		<Part>
			<PartNumber>1</PartNumber>
			<ETag>%s</ETag>
		</Part>
		<Part>
			<PartNumber>2</PartNumber>
			<ETag>%s</ETag>
		</Part>
	</CompleteMultipartUpload>`, etag2, etag3)

	req = httptest.NewRequest("POST", fmt.Sprintf("/%s/%s?uploadId=%s", bucketName, key, uploadID), bytes.NewReader([]byte(completeXML)))
	req.Header.Set("Content-Type", "application/xml")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("CompleteMultipartUpload failed: status=%d", w.Code)
	}

	// 5. 验证最终文件内容（应该是替换后的分片）
	expectedData := append(part1Data2, part2Data...)
	req = httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, key), nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GetObject failed: status=%d", w.Code)
	}

	if !bytes.Equal(w.Body.Bytes(), expectedData) {
		t.Errorf("Final file data mismatch: expected %d bytes, got %d bytes", len(expectedData), len(w.Body.Bytes()))
	}
}

// TestMultipartUploadUnorderedParts 测试无序分片上传
func TestMultipartUploadUnorderedParts(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"
	key := "test-unordered-parts"

	// 准备分片数据
	part1Data := []byte("part 1")
	part2Data := []byte("part 2")
	part3Data := []byte("part 3")
	expectedData := append(append(part1Data, part2Data...), part3Data...)

	// 1. 初始化分片上传
	req := httptest.NewRequest("POST", fmt.Sprintf("/%s/%s?uploads", bucketName, key), nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("InitiateMultipartUpload failed: status=%d", w.Code)
	}

	var initResult struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		UploadID string   `xml:"UploadId"`
	}
	xml.Unmarshal(w.Body.Bytes(), &initResult)
	uploadID := initResult.UploadID

	// 2. 无序上传分片（先上传part 3，再part 1，最后part 2）
	parts := []struct {
		partNumber int
		data       []byte
		etag       string
	}{
		{3, part3Data, ""},
		{1, part1Data, ""},
		{2, part2Data, ""},
	}

	for i, part := range parts {
		req = httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s?partNumber=%d&uploadId=%s", bucketName, key, part.partNumber, uploadID), bytes.NewReader(part.data))
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("UploadPart %d failed: status=%d", part.partNumber, w.Code)
		}
		parts[i].etag = w.Header().Get("ETag")
	}

	// 3. 完成分片上传（按part number顺序）
	completeXML := fmt.Sprintf(`<CompleteMultipartUpload>
		<Part>
			<PartNumber>1</PartNumber>
			<ETag>%s</ETag>
		</Part>
		<Part>
			<PartNumber>2</PartNumber>
			<ETag>%s</ETag>
		</Part>
		<Part>
			<PartNumber>3</PartNumber>
			<ETag>%s</ETag>
		</Part>
	</CompleteMultipartUpload>`, parts[1].etag, parts[2].etag, parts[0].etag)

	req = httptest.NewRequest("POST", fmt.Sprintf("/%s/%s?uploadId=%s", bucketName, key, uploadID), bytes.NewReader([]byte(completeXML)))
	req.Header.Set("Content-Type", "application/xml")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("CompleteMultipartUpload failed: status=%d", w.Code)
	}

	// 4. 验证最终文件内容（应该按part number顺序）
	req = httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, key), nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GetObject failed: status=%d", w.Code)
	}

	if !bytes.Equal(w.Body.Bytes(), expectedData) {
		t.Errorf("Final file data mismatch: expected %d bytes, got %d bytes", len(expectedData), len(w.Body.Bytes()))
	}
}
