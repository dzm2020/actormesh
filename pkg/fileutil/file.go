package fileutil

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

func LoadYamlFile(path string, value interface{}) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, value)
}

// ViperLoadConfigWithInclude 加载配置，支持 @include 递归引用
func ViperLoadConfigWithInclude(configPath string) (*viper.Viper, error) {
	// 1. 初始化 Viper 实例
	v := viper.New()
	v.SetConfigFile(configPath)
	// 2. 读取当前配置文件
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config %s failed: %w", configPath, err)
	}

	// 3. 提取 @include 字段（支持单个字符串或数组）
	var includes []string
	rawInclude := v.Get("include")
	if rawInclude != nil {
		switch val := rawInclude.(type) {
		case string:
			includes = []string{val}
		case []interface{}:
			for _, item := range val {
				if str, ok := item.(string); ok {
					includes = append(includes, str)
				}
			}
		}
	}

	// 4. 递归加载并合并所有 include 的配置
	baseDir := filepath.Dir(configPath)
	for _, incPath := range includes {
		// 路径处理：相对路径基于主配置文件所在目录
		fullIncPath := incPath
		if !filepath.IsAbs(incPath) {
			fullIncPath = filepath.Join(baseDir, incPath)
		}

		// 递归加载被引用的配置
		incV, err := ViperLoadConfigWithInclude(fullIncPath)
		if err != nil {
			return nil, fmt.Errorf("load include %s failed: %w", incPath, err)
		}

		// 合并到主 Viper（后加载覆盖先加载）
		if err := v.MergeConfigMap(incV.AllSettings()); err != nil {
			return nil, fmt.Errorf("merge include %s failed: %w", incPath, err)
		}
	}

	// 5. 可选：移除 @include 字段（避免污染最终配置）
	v.Set("include", nil)

	return v, nil
}
