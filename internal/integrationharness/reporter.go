package integrationharness

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"os"
	"path/filepath"
	"time"

	"ptah.run/internal/htmlstyle"
)

// ReportFormat represents the output format for reports
type ReportFormat string

const (
	FormatTXT    ReportFormat = "txt"
	FormatJSON   ReportFormat = "json"
	FormatHTML   ReportFormat = "html"
	FormatStdout ReportFormat = "stdout"
)

// Reporter handles generating reports in different formats
type Reporter struct {
	report *TestReport
}

// NewReporter creates a new reporter
func NewReporter(report *TestReport) *Reporter {
	return &Reporter{report: report}
}

// GenerateReport generates a report in the specified format and saves it to the given directory
func (r *Reporter) GenerateReport(format ReportFormat, outputDir string) error {
	timestamp := time.Now().Format("20060102-150405")
	filename := fmt.Sprintf("%s-report.%s", timestamp, string(format))
	fpath := filepath.Join(outputDir, filename)

	switch format {
	case FormatTXT:
		return r.generateTextReport(fpath)
	case FormatJSON:
		return r.generateJSONReport(fpath)
	case FormatHTML:
		return r.generateHTMLReport(fpath)
	case FormatStdout:
		return r.generateTextStreamReport(os.Stdout)
	default:
		return fmt.Errorf("unsupported report format: %s", format)
	}
}

// generateTextStreamReport generates a plain text stream report
func (r *Reporter) generateTextStreamReport(w io.Writer) error {
	// Header
	fmt.Fprintf(w, "PTAH MIGRATION LIBRARY INTEGRATION TEST REPORT\n")
	fmt.Fprintf(w, "===============================================\n\n")
	fmt.Fprintf(w, "Generated: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Fprintf(w, "Test Period: %s - %s\n",
		r.report.StartTime.Format("15:04:05"),
		r.report.EndTime.Format("15:04:05"))
	fmt.Fprintf(w, "Duration: %v\n\n",
		r.report.EndTime.Sub(r.report.StartTime).Round(time.Millisecond))

	// Summary
	fmt.Fprintf(w, "SUMMARY\n")
	fmt.Fprintf(w, "-------\n")
	fmt.Fprintf(w, "%s\n\n", r.report.Summary)

	// Statistics
	fmt.Fprintf(w, "STATISTICS\n")
	fmt.Fprintf(w, "----------\n")
	fmt.Fprintf(w, "Total Tests: %d\n", r.report.TotalTests)
	fmt.Fprintf(w, "Passed: %d\n", r.report.PassedTests)
	fmt.Fprintf(w, "Failed: %d\n", r.report.FailedTests)
	fmt.Fprintf(w, "Skipped: %d\n", r.report.SkippedTests)
	if executedTests := r.report.PassedTests + r.report.FailedTests; executedTests > 0 {
		successRate := float64(r.report.PassedTests) / float64(executedTests) * 100
		fmt.Fprintf(w, "Success Rate: %.1f%%\n", successRate)
	}
	fmt.Fprintf(w, "\n")

	// Detailed Results
	fmt.Fprintf(w, "DETAILED RESULTS\n")
	fmt.Fprintf(w, "----------------\n")

	for _, result := range r.report.Results {
		status := "✅ PASS"
		switch {
		case result.Skipped:
			status = "⏭️ SKIP"
		case !result.Success:
			status = "❌ FAIL"
		}

		fmt.Fprintf(w, "%s %s (%s) - %v\n",
			status, result.Name, result.Database, result.Duration.Round(time.Millisecond))
		fmt.Fprintf(w, "    Description: %s\n", result.Description)

		if !result.Success && result.Error != "" {
			fmt.Fprintf(w, "    Error: %s\n", result.Error)
		}
		if result.Skipped && result.SkipReason != "" {
			fmt.Fprintf(w, "    Skip: %s\n", result.SkipReason)
		}
		fmt.Fprintf(w, "\n")
	}

	// Failed Tests Summary
	if r.report.FailedTests > 0 {
		fmt.Fprintf(w, "FAILED TESTS SUMMARY\n")
		fmt.Fprintf(w, "--------------------\n")
		for _, result := range r.report.Results {
			if !result.Success && !result.Skipped {
				fmt.Fprintf(w, "❌ %s (%s)\n", result.Name, result.Database)
				fmt.Fprintf(w, "   Error: %s\n\n", result.Error)
			}
		}
	}

	return nil
}

// generateTextReport generates a plain text report
func (r *Reporter) generateTextReport(fpath string) error {
	file, err := os.Create(fpath)
	if err != nil {
		return err
	}
	defer file.Close()

	return r.generateTextStreamReport(file)
}

// generateJSONReport generates a JSON report
func (r *Reporter) generateJSONReport(fpath string) error {
	file, err := os.Create(fpath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(r.report)
}

// generateHTMLReport generates an HTML report
func (r *Reporter) generateHTMLReport(fpath string) error {
	file, err := os.Create(fpath)
	if err != nil {
		return err
	}
	defer file.Close()

	tmpl := template.Must(template.New("report").Funcs(template.FuncMap{
		"formatDuration": func(d time.Duration) string {
			return d.Round(time.Millisecond).String()
		},
		"formatTime": func(t time.Time) string {
			return t.Format("2006-01-02 15:04:05")
		},
		"successRate": func() float64 {
			executedTests := r.report.PassedTests + r.report.FailedTests
			if executedTests == 0 {
				return 0
			}
			return float64(r.report.PassedTests) / float64(executedTests) * 100
		},
		"statusLabel": func(result TestResult) string {
			if result.Skipped {
				return "SKIP"
			}
			if result.Success {
				return "PASS"
			}
			return "FAIL"
		},
		"statusClass": func(result TestResult) string {
			if result.Skipped {
				return "skip"
			}
			if result.Success {
				return "pass"
			}
			return "fail"
		},
		"stepClass": func(step TestStep) string {
			if step.Success {
				return "pass"
			}
			return "fail"
		},
		"stepLabel": func(step TestStep) string {
			if step.Success {
				return "PASS"
			}
			return "FAIL"
		},
	}).Parse(htmlTemplate))

	if _, err := io.WriteString(file, htmlstyle.Head("Ptah integration test report", reportCSS)); err != nil {
		return err
	}
	if err := tmpl.Execute(file, r.report); err != nil {
		return err
	}
	footer := htmlstyle.Footer("Rendered by Ptah from the integration suite. " +
		"This file is self-contained: opening it fetches nothing.")
	_, err = io.WriteString(file, footer+"</div></body>\n</html>\n")
	return err
}

// reportCSS is what this report adds to the shared appearance: the status
// classes and the expandable step list.
//
// The mapping from a result's status to a color lives here rather than on the
// result, for the same reason it does in migration/dbtest: the status is a fact
// about the run and the color is a fact about the page.
const reportCSS = `
.tag.pass { background: var(--ok-soft); border-color: transparent; color: var(--ok); }
.tag.fail { background: var(--danger-soft); border-color: transparent; color: var(--danger); }
.tag.skip { color: var(--text-mute); font-weight: 400; }
td.status { width: 1%; }
details { margin-top: 6px; }
summary { cursor: pointer; font: 500 11px var(--mono); letter-spacing: .08em; text-transform: uppercase; color: var(--text-mute); }
summary:hover { color: var(--text); }
.steps { list-style: none; margin: 8px 0 0; padding: 0 0 0 2px; display: grid; gap: 6px; }
.steps li { display: grid; gap: 2px; }
.step-head { display: flex; align-items: baseline; gap: 8px; }
.step-name { font-family: var(--mono); font-size: 13px; }
.step-duration { color: var(--text-mute); font-family: var(--mono); font-size: 12px; }
.note { color: var(--text-mute); font-size: 12.5px; }
.error { color: var(--danger); font-family: var(--mono); font-size: 12.5px; word-break: break-word; }
`

// htmlTemplate is the body between the shared head and the shared footer.
//
// The step list is a <details> element rather than a row toggled by an inline
// onclick handler. It needs no script, it works with the keyboard and with
// find-in-page, and it removes the one place a scenario name was interpolated
// into JavaScript.
const htmlTemplate = `<body><div class="page">
<h1>Integration test report</h1>
<div class="lede">{{printf "%.1f%%" successRate}} of executed tests passed {{"\u00b7"}} finished {{formatTime .EndTime}}</div>
<div class="stats">
<div class="stat"><div class="stat-n">{{.TotalTests}}</div><div class="stat-l">tests</div></div>
<div class="stat"><div class="stat-n">{{.PassedTests}}</div><div class="stat-l">passed</div></div>
<div class="stat"><div class="stat-n">{{.FailedTests}}</div><div class="stat-l">failed</div></div>
<div class="stat"><div class="stat-n">{{.SkippedTests}}</div><div class="stat-l">skipped</div></div>
</div>
<h2>Results</h2>
<div class="card"><div class="scroller"><table>
<thead><tr><th>Status</th><th>Test</th><th>Database</th><th>Duration</th><th>Detail</th></tr></thead>
<tbody>
{{range .Results}}
<tr>
<td class="status"><span class="tag {{statusClass .}}">{{statusLabel .}}</span></td>
<td class="name">{{.Name}}</td>
<td><span class="tag">{{.Database}}</span></td>
<td class="type">{{formatDuration .Duration}}</td>
<td class="comment">
{{.Description}}
{{if and (not .Success) (not .Skipped)}}<div class="error">{{.Error}}</div>{{end}}
{{if .Skipped}}<div class="note">{{.SkipReason}}</div>{{end}}
{{if .Steps}}
<details>
<summary>{{len .Steps}} steps</summary>
<ul class="steps">
{{range .Steps}}
<li>
<div class="step-head">
<span class="tag {{stepClass .}}">{{stepLabel .}}</span>
<span class="step-name">{{.Name}}</span>
<span class="step-duration">{{formatDuration .Duration}}</span>
</div>
{{if .Description}}<div class="note">{{.Description}}</div>{{end}}
{{if not .Success}}<div class="error">{{.Error}}</div>{{end}}
</li>
{{end}}
</ul>
</details>
{{end}}
</td>
</tr>
{{end}}
</tbody>
</table></div></div>
`
