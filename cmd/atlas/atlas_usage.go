package atlas

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

const atlasPreserveHelpAnnotation = "ptah.atlas.preserve-help"

func installAtlasUsageTree(cmd *cobra.Command) {
	if cmd.Annotations == nil {
		cmd.Annotations = map[string]string{}
	}
	if cmd.Annotations[atlasPreserveHelpAnnotation] == "" {
		cmd.SetHelpFunc(func(cmd *cobra.Command, _ []string) {
			_ = renderAtlasHelp(cmd)
		})
		cmd.SetUsageFunc(renderAtlasUsage)
	}
	for _, child := range cmd.Commands() {
		installAtlasUsageTree(child)
	}
}

func renderAtlasUsage(cmd *cobra.Command) error {
	out := cmd.OutOrStderr()
	useLine := atlasUseLine(cmd)
	fmt.Fprint(out, "Usage:")
	if cmd.Runnable() {
		fmt.Fprintf(out, "\n  %s", useLine)
	}
	if atlasShouldPrintSubcommandUsage(cmd, useLine) {
		fmt.Fprintf(out, "\n  %s [command]", atlasDisplayPath(cmd))
	}
	fmt.Fprintln(out)
	return nil
}

func renderAtlasHelp(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	description := strings.TrimSpace(cmd.Long)
	if description == "" {
		description = strings.TrimSpace(cmd.Short)
	}
	if description != "" {
		fmt.Fprint(out, description)
		fmt.Fprintln(out)
		fmt.Fprintln(out)
	}
	useLine := atlasUseLine(cmd)
	fmt.Fprint(out, "Usage:")
	if cmd.Runnable() {
		fmt.Fprintf(out, "\n  %s", useLine)
	}
	if atlasShouldPrintSubcommandUsage(cmd, useLine) {
		fmt.Fprintf(out, "\n  %s [command]", atlasDisplayPath(cmd))
	}
	if len(cmd.Aliases) > 0 {
		fmt.Fprintf(out, "\n\nAliases:\n  %s", cmd.NameAndAliases())
	}
	if cmd.HasExample() {
		fmt.Fprintf(out, "\n\nExamples:\n%s", cmd.Example)
	}
	if cmd.HasAvailableSubCommands() {
		fmt.Fprint(out, "\n\nAvailable Commands:")
		for _, child := range cmd.Commands() {
			if child.IsAvailableCommand() || child.Name() == "help" {
				fmt.Fprintf(out, "\n  %s %s", rpadAtlasHelp(child.Name(), child.NamePadding()), child.Short)
			}
		}
	}
	if cmd.HasAvailableLocalFlags() {
		fmt.Fprintf(out, "\n\nFlags:\n%s", trimAtlasHelp(cmd.LocalFlags().FlagUsages()))
	}
	if cmd.HasAvailableInheritedFlags() {
		fmt.Fprintf(out, "\n\nGlobal Flags:\n%s", trimAtlasHelp(cmd.InheritedFlags().FlagUsages()))
	}
	if cmd.HasHelpSubCommands() {
		fmt.Fprint(out, "\n\nAdditional help topics:")
		for _, child := range cmd.Commands() {
			if child.IsAdditionalHelpTopicCommand() {
				fmt.Fprintf(out, "\n  %s %s", rpadAtlasHelp(atlasDisplayPath(child), child.CommandPathPadding()), child.Short)
			}
		}
	}
	if cmd.HasAvailableSubCommands() {
		fmt.Fprintf(out, "\n\nUse \"%s [command] --help\" for more information about a command.", atlasDisplayPath(cmd))
	}
	fmt.Fprintln(out)
	return nil
}

func atlasUseLine(cmd *cobra.Command) string {
	useLine := atlasDisplayPath(cmd)
	if suffix := atlasUseSuffix(cmd.Use); suffix != "" {
		useLine += " " + suffix
	}
	if atlasShouldAppendFlags(cmd, useLine) {
		useLine += " [flags]"
	}
	return useLine
}

func atlasDisplayPath(cmd *cobra.Command) string {
	return cmd.CommandPath()
}

func atlasUseSuffix(use string) string {
	parts := strings.Fields(use)
	if len(parts) <= 1 {
		return ""
	}
	return strings.Join(parts[1:], " ")
}

func atlasShouldAppendFlags(cmd *cobra.Command, useLine string) bool {
	if cmd.DisableFlagsInUseLine {
		return false
	}
	if !cmd.HasAvailableFlags() {
		return false
	}
	if strings.Contains(useLine, "[flags]") {
		return false
	}
	return !cmd.HasAvailableSubCommands() || !strings.Contains(useLine, "[command]")
}

func atlasShouldPrintSubcommandUsage(cmd *cobra.Command, useLine string) bool {
	return cmd.HasAvailableSubCommands() && !strings.Contains(useLine, "[command]")
}

func rpadAtlasHelp(s string, padding int) string {
	template := fmt.Sprintf("%%-%ds", padding)
	return fmt.Sprintf(template, s)
}

func trimAtlasHelp(s string) string {
	return strings.TrimRight(s, " \t\r\n")
}
