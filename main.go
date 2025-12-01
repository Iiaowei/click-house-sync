// main 包是程序入口，负责运行 CLI。
package main

import "click-house-sync/cmd"

// main 执行根命令。
func main() {
	cmd.Execute()
}
