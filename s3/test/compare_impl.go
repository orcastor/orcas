package main

import (
	"fmt"
	"os/exec"
)

func main() {
	// Compare the two implementations
	fmt.Println("Comparing test/debug_sigv4.go and middleware/aws_auth.go implementations...")

	// Check buildCanonicalHeaders
	cmd1 := exec.Command("grep", "-A", "60", "func buildCanonicalHeaders", "test/debug_sigv4.go")
	out1, _ := cmd1.Output()

	cmd2 := exec.Command("grep", "-A", "60", "func buildCanonicalHeaders", "middleware/aws_auth.go")
	out2, _ := cmd2.Output()

	if string(out1) != string(out2) {
		fmt.Println("❌ buildCanonicalHeaders implementations differ!")
		fmt.Println("\nDebug version:")
		fmt.Println(string(out1))
		fmt.Println("\nMiddleware version:")
		fmt.Println(string(out2))
	} else {
		fmt.Println("✅ buildCanonicalHeaders implementations match")
	}
}
