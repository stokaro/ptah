//go:build !js

package assist

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"charm.land/bubbles/v2/key"
	"charm.land/bubbles/v2/textarea"
	tea "charm.land/bubbletea/v2"
	"charm.land/huh/v2"
	"charm.land/lipgloss/v2"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"ptah.run/internal/assistloop"
)

// The interactive surface at a terminal.
//
// It is an inline Bubble Tea program: the prompt is the only thing it repaints,
// and everything finished -- the question, the answer, the tool trace, the
// provenance footer -- is pushed into the terminal's scrollback, where it
// scrolls and can be selected and copied like any other command's output. The
// alternative, keeping the conversation inside the view, would cap it at the
// window height and repaint all of it on every token.
//
// A scripted run never reaches this file. `runChat` sends a pipe, a file or a
// test down the plain path in chat.go, byte for byte as before.
//
// Four facts about this stack decide the shape here, each measured before it
// was written rather than read off a signature:
//
//  1. `Program.Send` is ordered, lossless, and a no-op once the program has
//     exited. That is what carries the model's tokens from the worker
//     goroutine. A channel with a draining command loses the tail of the
//     stream, because the final message overtakes what is still buffered.
//  2. The approval reply channel has capacity 1 and is written at most once.
//     At capacity 0 the write inside Update blocks the whole event loop when
//     the worker has already gone, and the interface freezes.
//  3. Bubble Tea v2 does not quit on Ctrl-C. In raw mode it is an ordinary key
//     press, so this model owns the decision: cancel a call in flight, leave
//     at the prompt.
//  4. An embedded `huh.Form` keeps the older Bubble Tea shape -- `Init`,
//     `Update` returning `huh.Model`, and a `View` returning a string -- so it
//     is held as a `*huh.Form` and driven by hand rather than stored in a
//     `tea.Model` field.

// tuiSession is what the program needs from the rest of the surface. It is an
// interface so the model can be driven without a provider or a running server.
type tuiSession interface {
	// ask runs one request to completion, streaming text to onText.
	ask(ctx context.Context, request string, onText func(string)) (*assistloop.Result, error)
}

// approvalRequest is one elicitation waiting for a person.
type approvalRequest struct {
	message string
	// reply carries the decision back to the goroutine blocked inside the MCP
	// callback. Capacity 1, written at most once; see fact 2 above.
	reply chan string
}

// Decisions the approval form offers, in the order it lists them. The values
// are what terminalApprover already answers with, so the two surfaces agree on
// what a decision means without a second vocabulary.
const (
	decisionOnce    = "once"
	decisionSession = "session"
	decisionNo      = "no"
)

type (
	fragmentMsg struct{ text string }
	doneMsg     struct {
		result *assistloop.Result
		err    error
	}
	// printedMsg acknowledges that the previous scrollback line reached the
	// event loop, so the next may go. One print is in flight at a time:
	// each command runs in its own goroutine and two of them reorder.
	printedMsg struct{}
	// formDoneMsg carries the decision rather than a canceled flag: the bound
	// pointer follows the cursor rather than the submission, so on an abort it
	// holds whatever happened to be highlighted.
	formDoneMsg struct{ decision string }
)

type phase int

const (
	atPrompt phase = iota
	thinking
	awaitingApproval
)

// tuiModel is the whole interactive surface.
type tuiModel struct {
	// program is stored between NewProgram and Run, and read by the worker
	// goroutine. Never sent to before Run, which would block.
	program atomic.Pointer[tea.Program]

	session tuiSession
	input   textarea.Model
	spinner int

	phase phase
	// answer is everything the model has streamed for the question in flight.
	// It is held whole rather than flushed line by line, because Markdown is
	// rendered as a document: a list, a fenced block and an emphasis run all
	// need more than the line they start on. The view shows its tail while it
	// arrives; the scrollback gets it rendered, once, when it is complete.
	answer strings.Builder

	// history is this session's questions, newest last, walked with Up and
	// Down. It is not written to disk: the conversation is already saved under
	// .ptah/sessions unless --ephemeral says otherwise, and a second copy of
	// what was typed would not honor that flag.
	history []string
	at      int
	draft   string

	pending *approvalRequest
	form    *huh.Form
	// decision is a heap pointer because Update has a value receiver on the
	// form's side: binding to a field of a copied model loses the answer with
	// no error at all.
	decision *string

	cancel   context.CancelFunc
	dropDone bool

	queue    []string
	inFlight bool

	trace bool
	width int
	info  tuiInfo
}

// newTUIModel builds the model. The program is attached afterwards, by run.
func newTUIModel(session tuiSession, trace bool) *tuiModel {
	input := textarea.New()
	// textinput cannot hold a newline -- its sanitizer replaces one with a
	// space and has no setter -- so the multi-line requirement picks textarea.
	input.ShowLineNumbers = false
	input.DynamicHeight = true
	input.MinHeight, input.MaxHeight = 1, 10
	// Enter submits, so a deliberate newline needs its own key. Ctrl-J is a
	// plain control byte every terminal sends without negotiating anything.
	input.KeyMap.InsertNewline = key.NewBinding(
		key.WithKeys("ctrl+j"),
		key.WithHelp("ctrl+j", "newline"),
	)
	// The default styles fill each line out to the width with a background,
	// which reads as a black band across the terminal. This surface wants a
	// plain line, so every style the component paints is cleared.
	plain := textarea.DefaultStyles(true)
	bare := lipgloss.NewStyle()
	plain.Focused.Base, plain.Focused.CursorLine, plain.Focused.EndOfBuffer = bare, bare, bare
	plain.Blurred.Base, plain.Blurred.CursorLine, plain.Blurred.EndOfBuffer = bare, bare, bare
	input.SetStyles(plain)
	// The real terminal cursor is drawn through tea.View.Cursor below, so the
	// component's own virtual cursor and the blink chain that drives it are
	// not needed: the terminal blinks it.
	input.SetVirtualCursor(false)
	// The marker is the component's, not something this file draws in front of
	// it: the cursor is placed in the component's own coordinates, so a marker
	// printed separately left the cursor two columns to the left of the text,
	// sitting on the marker and hiding it on an empty prompt.
	input.SetPromptFunc(promptWidth, func(info textarea.PromptInfo) string {
		return promptMarker(info.LineNumber)
	})
	input.Focus()

	return &tuiModel{session: session, input: input, trace: trace, decision: new(string)}
}

func (m *tuiModel) Init() tea.Cmd { return nil }

// send delivers a message from the worker goroutine. After the program has
// exited it does nothing, which is what lets a worker outlive the interface
// without hanging on it.
func (m *tuiModel) send(msg tea.Msg) {
	if p := m.program.Load(); p != nil {
		p.Send(msg)
	}
}

// say queues lines for the scrollback.
func (m *tuiModel) say(lines ...string) tea.Cmd {
	m.queue = append(m.queue, lines...)
	return m.pump()
}

// pump prints the next queued line, one at a time.
//
// tea.Sequence rather than tea.Batch: the acknowledgement must not overtake
// the print it acknowledges.
func (m *tuiModel) pump() tea.Cmd {
	if m.inFlight || len(m.queue) == 0 {
		return nil
	}
	// The whole queue in one print, not one print per line. Each Printf
	// command runs in its own goroutine and emits its own line break, so a
	// line at a time both doubled the spacing and left two prints racing to
	// reach the event loop.
	block := strings.Join(m.queue, "\n")
	m.queue = nil
	m.inFlight = true
	return tea.Sequence(
		tea.Printf("%s", block),
		func() tea.Msg { return printedMsg{} },
	)
}

// approve is the approval handler the MCP client calls, on the worker
// goroutine, blocking until the person decides.
func (m *tuiModel) approve(ctx context.Context, request *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
	pending := &approvalRequest{
		message: request.Params.Message,
		reply:   make(chan string, 1),
	}
	m.send(pending)

	select {
	case decision := <-pending.reply:
		switch decision {
		case decisionOnce:
			return accepted("allow once"), nil
		case decisionSession:
			return accepted("allow for this session"), nil
		}
		return &mcp.ElicitResult{Action: "decline"}, nil
	case <-ctx.Done():
		// Never wait forever on an answer the interface may never give: the
		// person may have canceled the call or left.
		return &mcp.ElicitResult{Action: "cancel"}, nil
	}
}

// startCall runs one question on a goroutine of its own.
func (m *tuiModel) startCall(request string) {
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.phase = thinking
	m.dropDone = false
	m.answer.Reset()

	go func() {
		result, err := m.session.ask(ctx, request, func(fragment string) {
			m.send(fragmentMsg{text: fragment})
		})
		m.send(doneMsg{result: result, err: err})
	}()
}

func (m *tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyPressMsg:
		return m.key(msg)

	case tea.PasteMsg:
		// The sanitizer turns each \r and each \n into a newline on its own,
		// so a paste from Windows or a web page would double every break.
		clean := tea.PasteMsg{Content: strings.ReplaceAll(msg.Content, "\r\n", "\n")}
		input, cmd := m.input.Update(clean)
		m.input = input
		return m, cmd

	case printedMsg:
		m.inFlight = false
		return m, m.pump()

	case fragmentMsg:
		return m, m.fragment(msg.text)

	case *approvalRequest:
		return m, m.openForm(msg)

	case formDoneMsg:
		return m, m.answerForm(msg.decision)

	case doneMsg:
		return m, m.finish(msg)

	case spinnerTickMsg:
		m.spinner++
		return m, m.tick()

	case tea.WindowSizeMsg:
		// Without a width the textarea has no line to wrap against and the
		// renderer repaints every frame over a view whose trailing space keeps
		// changing: measured at 21 erase-to-end sequences a second on an idle
		// prompt. The 2 is the width of the "> " this surface draws itself.
		m.width = msg.Width
		m.input.SetWidth(max(msg.Width, 24))
		return m, nil
	}

	// A form drives itself with messages of its own: Enter on a select produces
	// huh's nextFieldMsg, and the form submits only when it receives that back.
	// Dropping it here left the approval prompt on screen forever -- the arrow
	// keys moved the cursor, Enter did nothing, and the next thing typed went
	// into the form's filter. Nothing else is forwarded, because the textarea
	// re-arms its own timers when it is handed messages it did not ask for.
	if m.form != nil {
		return m, m.driveForm(msg)
	}

	// Everything else is dropped, deliberately. Forwarding unhandled messages
	// to the textarea makes it re-arm the cursor-blink timer it schedules for
	// itself, and with the virtual cursor off nothing ever consumes the
	// result: measured at 80 repaints a second on an idle prompt, growing with
	// uptime. The component is given exactly what it needs -- key presses and
	// pastes, above -- and nothing else.
	return m, nil
}

// fragment adds streamed text and prints whatever has settled.
//
// A Markdown block is complete once a blank line ends it, so that much can be
// rendered and pushed to the scrollback while the rest is still arriving. What
// is left is rendered live in the view, which is what makes the answer appear
// formatted as it is written rather than raw and then replaced.
func (m *tuiModel) fragment(text string) tea.Cmd {
	m.answer.WriteString(text)

	// Leading blank lines carry nothing, and left in place they jam the whole
	// stream: a model opens its answer with a newline or two, the block cut
	// lands on the first of them and yields an empty `settled` the caller
	// cannot use, and the line cut refuses a buffer whose first line is blank.
	// Neither ever clears them, so nothing was ever sent until the turn ended.
	// This is the one that mattered -- every other rule here only decides where
	// a stream that is running gets cut.
	if trimmed := strings.TrimLeft(m.answer.String(), "\n"); trimmed != m.answer.String() {
		m.answer.Reset()
		m.answer.WriteString(trimmed)
	}

	// A finished block first: it carries its own spacing, and the blank line
	// that ended it is the signal that the block is done.
	if settled, rest := splitRenderable(m.answer.String()); settled != "" {
		m.answer.Reset()
		m.answer.WriteString(rest)
		return m.say(renderAnswer(settled, m.width)...)
	}

	// Otherwise whatever prose has completed a line. This is what makes an
	// answer arrive as it is written: waiting for the end of the block shows a
	// paragraph all at once, and showing the unfinished part in a pane of its
	// own draws the same text twice in two places.
	if settled, rest := splitProse(m.answer.String()); settled != "" {
		m.answer.Reset()
		m.answer.WriteString(rest)
		return m.say(renderProse(settled, m.width)...)
	}

	return m.pump()
}

// openForm puts the approval choice on screen.
func (m *tuiModel) openForm(request *approvalRequest) tea.Cmd {
	m.pending = request
	m.phase = awaitingApproval
	*m.decision = decisionNo

	// Esc means deny. huh binds Quit to Ctrl-C alone, and for a prompt that
	// guards a write that is not enough.
	keys := huh.NewDefaultKeyMap()
	keys.Quit = key.NewBinding(key.WithKeys("esc", "ctrl+c"))

	m.form = huh.NewForm(
		huh.NewGroup(
			huh.NewSelect[string]().
				Title(request.message).
				Options(
					huh.NewOption("Allow once", decisionOnce),
					huh.NewOption("Allow for this session", decisionSession),
					huh.NewOption("No", decisionNo),
				).
				Value(m.decision),
		),
	).WithKeyMap(keys).WithShowHelp(true)

	// The defaults are tea.Quit and tea.Interrupt, which would take the host
	// program down with the form.
	m.form.SubmitCmd = func() tea.Msg { return formDoneMsg{decision: *m.decision} }
	m.form.CancelCmd = func() tea.Msg { return formDoneMsg{decision: decisionNo} }

	return m.form.Init()
}

// driveForm hands one message to the approval form and keeps whatever it
// becomes. The form is held as a *huh.Form rather than in a tea.Model field
// because it carries the older Bubble Tea shape, so the assertion is here.
func (m *tuiModel) driveForm(msg tea.Msg) tea.Cmd {
	form, cmd := m.form.Update(msg)
	if updated, ok := form.(*huh.Form); ok {
		m.form = updated
	}
	return cmd
}

// answerForm answers the waiting goroutine and puts the prompt back.
func (m *tuiModel) answerForm(decision string) tea.Cmd {
	if m.pending != nil {
		m.pending.reply <- decision // capacity 1: cannot block
		m.pending = nil
	}
	m.form = nil
	m.phase = thinking
	return m.say(noticeStyle.Render("  " + decisionWord[decision]))
}

var decisionWord = map[string]string{
	decisionOnce:    "allowed once",
	decisionSession: "allowed for this session",
	decisionNo:      "declined",
}

// finish prints what the run produced and returns to the prompt.
func (m *tuiModel) finish(msg doneMsg) tea.Cmd {
	m.phase = atPrompt
	m.pending = nil
	m.form = nil
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	if m.dropDone {
		m.dropDone = false
		return m.pump()
	}

	// Rendered here rather than as it arrived: Markdown is a document, and a
	// line at a time cannot know it is inside a list or a fenced block.
	lines := renderAnswer(m.answer.String(), m.width)
	m.answer.Reset()
	lines = append(lines, tuiReport(msg.result, msg.err, traced(m.trace))...)
	// And a blank line after the footer, so the next question does not start
	// on the line under it.
	return m.say(append(lines, "")...)
}

func (m *tuiModel) key(press tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	if press.Keystroke() == "ctrl+c" && m.phase != awaitingApproval {
		return m.interrupt()
	}

	if m.form != nil {
		return m, m.driveForm(press)
	}
	if m.phase != atPrompt {
		return m, nil
	}

	switch press.Keystroke() {
	case "enter":
		return m, m.submit()
	case "ctrl+d":
		return m, tea.Quit
	case "up", "down":
		if m.input.LineCount() == 1 {
			m.walk(press.Keystroke())
			return m, nil
		}
	case "tab":
		m.complete()
		return m, nil
	}

	input, cmd := m.input.Update(press)
	m.input = input
	return m, cmd
}

// interrupt cancels a call in flight, or leaves when nothing is running.
func (m *tuiModel) interrupt() (tea.Model, tea.Cmd) {
	if m.phase == atPrompt {
		return m, tea.Quit
	}
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	m.phase = atPrompt
	m.pending = nil
	m.form = nil
	// What arrived before the cancel is kept and rendered: a partial answer is
	// still an answer, and dropping it would throw away what was paid for.
	canceled := renderAnswer(m.answer.String(), m.width)
	m.answer.Reset()
	// The worker still delivers one doneMsg after this; without the flag the
	// surface would print both "canceled" and a context-canceled footer.
	m.dropDone = true
	return m, m.say(append(canceled, noticeStyle.Render("  canceled"))...)
}

// submit sends the typed question, or handles a directive.
func (m *tuiModel) submit() tea.Cmd {
	request := strings.TrimSpace(m.input.Value())
	if request == "" {
		return nil
	}
	m.input.Reset()
	m.remember(request)

	if strings.HasPrefix(request, "/") {
		leave, lines, trace := tuiDirective(request, m.trace, m.info)
		m.trace = trace
		echo := echoStyle.Render("> " + request)
		said := append([]string{echo, ""}, lines...)
		if leave {
			return tea.Sequence(m.say(said...), tea.Quit)
		}
		return m.say(append(said, "")...)
	}

	// The question is set apart on both sides: from the answer above it, and
	// from the spinner that replaces the prompt under it. Without the second
	// one the spinner sits on the line directly below the question, which
	// reads as part of it.
	cmd := m.say(echoStyle.Render("> "+request), "")
	m.startCall(request)
	return tea.Batch(cmd, m.tick())
}

// remember appends to the history, skipping a repeat of the newest entry.
func (m *tuiModel) remember(line string) {
	if len(m.history) == 0 || m.history[len(m.history)-1] != line {
		m.history = append(m.history, line)
	}
	m.at = len(m.history)
	m.draft = ""
}

// walk moves through the history, keeping whatever was half-typed so stepping
// back down returns it rather than an empty line.
func (m *tuiModel) walk(direction string) {
	if len(m.history) == 0 {
		return
	}
	if m.at == len(m.history) {
		m.draft = m.input.Value()
	}
	if direction == "up" && m.at > 0 {
		m.at--
	}
	if direction == "down" && m.at < len(m.history) {
		m.at++
	}
	m.input.Reset()
	if m.at == len(m.history) {
		m.input.InsertString(m.draft)
		return
	}
	m.input.InsertString(m.history[m.at])
}

// complete finishes a directive when exactly one matches. The textarea has no
// suggestion API, so Tab is the caller's to handle.
func (m *tuiModel) complete() {
	line := m.input.Value()
	if !strings.HasPrefix(line, "/") {
		return
	}
	matches := directivesWithPrefix(line)
	if len(matches) != 1 {
		return
	}
	m.input.Reset()
	m.input.InsertString(matches[0] + " ")
}

type spinnerTickMsg struct{}

func (m *tuiModel) tick() tea.Cmd {
	if m.phase == atPrompt {
		return nil
	}
	return tea.Tick(spinnerPeriod, func(time.Time) tea.Msg { return spinnerTickMsg{} })
}

func (m *tuiModel) View() tea.View {
	// No AltScreen anywhere: inline is the default in this version, and the
	// conversation belongs in the scrollback.
	if m.form != nil {
		return tea.NewView(m.form.View())
	}
	switch m.phase {
	case thinking, awaitingApproval:
		// The spinner, and nothing else. Painting the text that has not settled
		// yet showed it twice: once in a block the view redraws in place, and
		// again when it reached the scrollback, where it is laid out properly
		// and lands somewhere else on screen. The jump is what a reader
		// notices, and it made the spinner invisible as well -- the preview
		// filled the line the spinner would have been on, so a model that
		// thought for ten seconds looked like a model that did not think.
		//
		// An answer still arrives in pieces: each block goes to the scrollback
		// as it finishes, and what has not finished waits behind the spinner.
		body := "  " + spinnerStyle.Render(spinnerFrame(m.spinner)) + " thinking"
		return tea.NewView(body + "\n" + hintStyle.Render("  esc or ctrl+c to cancel"))
	default:
		view := tea.NewView(m.input.View())
		view.Cursor = m.input.Cursor()
		return view
	}
}
