package jobs

import (
	"testing"
)

func TestProcessWordCount(t *testing.T) {
	tests := []struct {
		name string
		text string
		want uint64
	}{
		{
			"empty input",
			"",
			0,
		},
		{
			name: "whitespace only",
			text: " \t\n ",
			want: 0,
		},
		{
			name: "two words",
			text: "hello world",
			want: 2,
		},
		{
			name: "mixed whitespace",
			text: "  hello\tworld\nagain  ",
			want: 3,
		},
		{
			name: "unicode whitespace",
			text: "hello\u2003world",
			want: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Process(tt.text)

			if result.WordCount != tt.want {
				t.Errorf("Process(%q).WordCount = %d, want = %d ",
					tt.text,
					result.WordCount,
					tt.want,
				)
			}
		})
	}
}

func TestProcessSHA256(t *testing.T) {
	tests := []struct {
		name string
		text string
		want string
	}{
		{
			name: "empty input",
			text: "",
			want: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name: "simple text",
			text: "abc",
			want: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
		},
		{
			name: "preserves trailing newline",
			text: "abc\n",
			want: "edeaaff3f1774ad2888673770c6d64097e391bc362d7d6fb34982ddf0efd18cb",
		},
		{
			name: "preserves repeated spaces",
			text: "a  b",
			want: "6e12db73209a66d147a67a15868bdb4b8ae57b884d4731310b62f82a7d67611e",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Process(tt.text)

			if result.SHA256 != tt.want {
				t.Errorf("Process(%q).SHA256 = %q, want = %q",
					tt.text,
					result.SHA256,
					tt.want,
				)
			}
		})
	}
}

func TestProcessWithContext(t *testing.T) {
	t.Skip()
}

func TestProcessWithContextCancelled(t *testing.T) {
	t.Skip()
}

func TestProcessWithContextDeadlineExceeded(t *testing.T) {
	t.Skip()
}
