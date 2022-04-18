package nolog

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/nolog-io/nolog-golang-lib/generated/source/proto/main/go/io/nolog/net/api/auth"
	"github.com/nolog-io/nolog-golang-lib/generated/source/proto/main/go/io/nolog/net/api/writespec"
	"google.golang.org/protobuf/proto"
)

type error_bit int

const (
	ERROR_BIT_DO_NOT_USE                      error_bit = iota
	ERROR_BIT_MULTIPLE_INITIALIZATION         error_bit = 1 << (iota - 1)
	ERROR_BIT_CLOSING_CLOSED_TRACKER          error_bit = 1 << (iota - 1)
	ERROR_BIT_OPEN_CHILD_OF_CLOSED_TRACKER    error_bit = 1 << (iota - 1)
	ERROR_BIT_CLOSING_TRACKED_AFTER_PARENT    error_bit = 1 << (iota - 1)
	ERROR_BIT_USED_UNREGISTERED_ALERT         error_bit = 1 << (iota - 1)
	ERROR_BIT_DUPLICATE_OBJECTIVE             error_bit = 1 << (iota - 1)
	ERROR_BIT_DUPLICATE_DEPENDENCY            error_bit = 1 << (iota - 1)
	ERROR_BIT_GC_BEFORE_CLOSING               error_bit = 1 << (iota - 1)
	ERROR_BIT_EMPTY_API_KEY                   error_bit = 1 << (iota - 1)
	ERROR_BIT_INVALID_API_KEY                 error_bit = 1 << (iota - 1)
	ERROR_BIT_MISSING_DEPENDENCY_TO_START_DEP error_bit = 1 << (iota - 1)
	ERROR_BIT_USED_UNREGISTERED_DEPENDENCY    error_bit = 1 << (iota - 1)
	ERROR_BIT_INCORRECT_API_USAGE             error_bit = 1 << (iota - 1)
	ERROR_BIT_USED_ALERT_DOUBLE_REGISTERED    error_bit = 1 << (iota - 1)
)

type notify_bits int

const (
	NOTIFY_BIT_DO_NOT_USE           notify_bits = iota
	NOTIFY_BIT_SERVICE_ID_MISSING   notify_bits = 1 << (iota - 1)
	NOTIFY_BIT_INSTANCE_ID_MISSING  notify_bits = 1 << (iota - 1)
	NOTIFY_BIT_FORGOT_TO_INITIALIZE notify_bits = 1 << (iota - 1)
	NOTIFY_BIT_EMPTY_API_KEY        notify_bits = 1 << (iota - 1)
	NOTIFY_BIT_INVALID_API_KEY      notify_bits = 1 << (iota - 1)
	NOTIFY_BIT_INCORRECT_API_USAGE  notify_bits = 1 << (iota - 1)
)

type error_handler struct {
	errors      []string
	error_bits  error_bit
	notify_bits notify_bits
	lock        sync.Mutex
}

func trim(field string, length int, cleanse bool) string {
	if cleanse {
		var newField strings.Builder
		var validChars = "ABCDEFGHIJKLMNOPQRSTUVWYXZabcdefghijklmnopqrstuvwxyz1234567890/.";
		for _, c := range field {
			if !strings.ContainsRune(validChars, c) {
				continue
			}
			newField.WriteRune(c);
		}
		field = strings.ToLower(newField.String())
	}
	if len(field) == 0 {
		return "UNKNOWN";
	}
	if len(field) > length {
		field = field[0:length - 3] + "...";
	}
	return field;
}

func new_error_handler() *error_handler {
	return &error_handler{
		errors:      []string{},
		error_bits:  0,
		notify_bits: 0,
		lock:        sync.Mutex{},
	}
}

// NOTE (RE: Reciever Name Warnings)
// In Go, using this or self is not part of golangs naming best practices.
// However; given that NoLog needs to maintain consistency and readability
// across a host of languages, we deliberately made the decision to use
// self here to make the code more comparable across different languages.

func (self *error_handler) safe_get_errors() (error_bit, []string) {
	self.lock.Lock()
	bits := self.error_bits
	errors := make([]string, len(self.errors))
	copy(errors, self.errors)
	self.lock.Unlock()
	return bits, errors
}

func (self *error_handler) safe_add_error(err string, bit error_bit) {
	if bit == ERROR_BIT_DO_NOT_USE {
		log.Fatal("Found invalid error bit: ERROR_BIT_DO_NOT_USE")
	}
	self.lock.Lock()
	if self.error_bits&bit > 0 {
		self.lock.Unlock()
		return
	}
	self.error_bits |= bit
	self.errors = append(self.errors, err)
	self.lock.Unlock()
}

func (self *error_handler) safe_notify(notification string, bit notify_bits) {
	if bit == NOTIFY_BIT_DO_NOT_USE {
		log.Fatal("Found invalid notify bit: NOTIFY_BIT_DO_NOT_USE")
	}
	self.lock.Lock()
	if self.notify_bits&bit > 0 {
		self.lock.Unlock()
		return
	}
	self.notify_bits |= bit
	self.lock.Unlock()
	log.Print("NoLog Notification: Initialization failed because " + notification)
}

func (e *error_handler) safe_has_error() bool {
	e.lock.Lock()
	has_error := e.error_bits > 0
	e.lock.Unlock()
	return has_error
}

type alert struct {
	max_invocation_count int
	invocation_count     int
	lock                 sync.Mutex
	count                int64
	samples              []string
	last_count           int64
	last_samples         []string
	parent               interface{}
	core_alert           string
	last_timestamp       int64
}

func new_alert(max_invocation_count int, core_alert string, parent interface{}) *alert {
	return &alert{
		max_invocation_count: max_invocation_count,
		invocation_count:     0,
		lock:                 sync.Mutex{},
		count:                0,
		samples:              []string{},
		last_count:           0,
		last_samples:         []string{},
		parent:               parent,
		core_alert:           trim(core_alert, 200, false),
	}
}

func (self *alert) get_core_alert() string {
	return self.core_alert
}

func (self *alert) safe_with_context(caller interface{}, context string) bool {
	if caller != self.parent {
		return false
	}
	self.lock.Lock()
	self.count++
	if len(self.samples) == 0 || (len(self.samples) < 3 && self.count%100 == 0) {
		context = trim(context, 1000, false)
		self.samples = append(self.samples, context)
	}
	self.lock.Unlock()
	return true
}

func (self *alert) safe_sample_alerts() (bool, []string, int64, int64) {
	self.lock.Lock()
	if self.invocation_count == 0 {
		if self.count == 0 {
			self.lock.Unlock()
			return false, nil, 0, 0
		}
		self.last_samples = self.samples
		self.last_count = self.count
		self.last_timestamp = time.Now().Unix()
		self.samples = []string{}
		self.count = 0
	}
	self.invocation_count = (self.invocation_count + 1) % self.max_invocation_count
	last_samples := make([]string, len(self.last_samples))
	copy(last_samples, self.last_samples)
	last_count := self.last_count
	timestamp := self.last_timestamp
	self.lock.Unlock()
	return true, last_samples, last_count, timestamp
}

type base_counter struct {
	name            string
	action          string
	lock            sync.Mutex
	nolog_init      bool
	failure_window  int
	successful      int
	failing         int
	success_history []int
	failing_history []int
	errors          *error_handler
	parent          *base_counter
	children        map[string]*base_counter
	alerts          map[string]*alert
}

func new_base_counter(name, action string, nolog_init bool, failure_window int, parent *base_counter) *base_counter {
	bc := &base_counter{
		name:            name,
		action:          action,
		lock:            sync.Mutex{},
		nolog_init:      nolog_init,
		failure_window:  failure_window,
		successful:      0,
		failing:         0,
		success_history: []int{},
		failing_history: []int{},
		parent:          parent,
		children:        map[string]*base_counter{},
		alerts:          map[string]*alert{},
	}
	if parent != nil {
		bc.errors = parent.errors
	} else {
		bc.errors = new_error_handler()
	}
	return bc
}

func (self *base_counter) safe_with_alert(core_alert string, max_invocation_count int) *alert {
	self.lock.Lock()
	if _, ok := self.alerts[core_alert]; ok {
		self.lock.Unlock()
		return nil
	}
	a := new_alert(max_invocation_count, core_alert, self)
	self.alerts[core_alert] = a
	self.lock.Unlock()
	return a
}

func (self *base_counter) proto_data() (string, string, bool, int, int, []*base_counter, []*alert) {
	self.lock.Lock()
	successful := self.successful
	failing := self.failing
	self.success_history = append(self.success_history, self.successful)
	self.failing_history = append(self.failing_history, self.failing)
	self.failing = 0
	self.successful = 0
	history_size := len(self.failing_history)
	if history_size > self.failure_window {
		newStartIndex := history_size - self.failure_window
		self.success_history = self.success_history[newStartIndex:history_size]
		self.failing_history = self.failing_history[newStartIndex:history_size]
	}
	success := true
	for _, failed := range self.failing_history {
		if failed > 0 {
			success = false
			break
		}
	}
	children := make([]*base_counter, 0, len(self.children))
	for _, v := range self.children {
		children = append(children, v)
	}
	alerts := make([]*alert, 0, len(self.alerts))
	for _, v := range self.alerts {
		alerts = append(alerts, v)
	}
	self.lock.Unlock()
	return self.name, self.action, success, successful, failing, children, alerts
}

func (self *base_counter) safe_add_child(name, action string, failure_window int) *base_counter {
	key := name + ":" + action
	self.lock.Lock()
	bc, ok := self.children[key]
	if ok {
		self.errors.safe_add_error(
			fmt.Sprintf("Called AddDependency multiple times with same name:action (%s)", key),
			ERROR_BIT_DUPLICATE_DEPENDENCY)
	} else {
		bc = new_base_counter(name, action, self.nolog_init, failure_window, self)
		self.children[key] = bc
	}
	self.lock.Unlock()
	return bc
}

func (self *base_counter) safe_update_init(is_initialized bool) {
	self.lock.Lock()
	self.nolog_init = is_initialized
	children := make([]*base_counter, 0, len(self.children))
	for _, v := range self.children {
		children = append(children, v)
	}
	self.lock.Unlock()
	for _, child := range children {
		child.safe_update_init(is_initialized)
	}
}

func (self *base_counter) safe_increment(inc_success int, inc_fail int) {
	self.lock.Lock()
	if inc_success > 0 {
		self.successful += inc_success
	}
	if inc_fail > 0 {
		self.failing += inc_fail
	}
	self.lock.Unlock()
}

func (self *base_counter) safe_start() *tracker {
	self.lock.Lock()
	if !self.nolog_init {
		self.lock.Unlock()
		self.errors.safe_notify(
			fmt.Sprintf("Did you forget to Initialize NoLog? Found %s.start() call before nolog.Initialize()", self.name),
			NOTIFY_BIT_FORGOT_TO_INITIALIZE)
		return disabled_tracker
	}
	self.lock.Unlock()
	return new_tracker(self)
}

type tracker struct {
	disabled      bool
	parent_closed bool
	closed        bool
	parent        *base_counter
	children      []*tracker
	lock          sync.Mutex
}

func new_tracker(parent *base_counter) *tracker {
	t := &tracker{
		lock:          sync.Mutex{},
		parent:        parent,
		parent_closed: false,
		closed:        false,
		disabled:      false,
		children:      []*tracker{},
	}
	runtime.SetFinalizer(t, func(t *tracker) {
		if t.disabled {
			return
		}
		t.unsafe_close_children()
		if !t.closed {
			t.parent.errors.safe_add_error(
				"Tracker garbage collected before closing. Did you forget to success()/fail()?",
				ERROR_BIT_GC_BEFORE_CLOSING)
		}
	})
	return t
}

var (
	disabled_tracker = &tracker{disabled: true}
)

func (self *tracker) safe_parent_closed() {
	self.lock.Lock()
	self.parent_closed = true
	closed := self.closed
	self.lock.Unlock()
	if !closed {
		self.parent.errors.safe_add_error(
			"Bad Monitoring State: Objective Tracker closed before DependencyTracker.",
			ERROR_BIT_OPEN_CHILD_OF_CLOSED_TRACKER)
	}
}

func (self *tracker) unsafe_close_children() {
	for _, child := range self.children {
		child.safe_parent_closed()
	}
	self.children = nil
}

func (self *tracker) safe_success() {
	if self.disabled {
		return
	}
	self.lock.Lock()
	self.unsafe_close_children()
	if self.closed {
		self.lock.Unlock()
		self.parent.errors.safe_add_error(
			"Bad Monitoring State: Tracker closed twice, success() called on closed Tracker.",
			ERROR_BIT_CLOSING_CLOSED_TRACKER)
		return
	}
	self.closed = true
	if self.parent_closed {
		self.lock.Unlock()
		self.parent.errors.safe_add_error(
			"Bad Monitoring State: Tried to close DependencyTracker after closing ObjectiveTracker.",
			ERROR_BIT_CLOSING_TRACKED_AFTER_PARENT)
		return
	}
	self.lock.Unlock()
	self.parent.safe_increment(1, 0)
}

func (self *tracker) safe_fail(a *alert, context string) {
	if self.disabled {
		return
	}
	self.lock.Lock()
	self.unsafe_close_children()
	if self.closed {
		self.lock.Unlock()
		self.parent.errors.safe_add_error(
			"Bad Monitoring State: Tracker closed twice, fail() called on closed Tracker.",
			ERROR_BIT_CLOSING_CLOSED_TRACKER)
		return
	}
	self.closed = true
	if self.parent_closed {
		self.lock.Unlock()
		self.parent.errors.safe_add_error(
			"Bad Monitoring State: Tried to close DependencyTracker after closing ObjectiveTracker.",
			ERROR_BIT_CLOSING_TRACKED_AFTER_PARENT)
		return
	}
	self.lock.Unlock()
	self.parent.safe_increment(0, 1)
	if a != nil {
		if !a.safe_with_context(self.parent, context) {
			self.parent.errors.safe_add_error(
				fmt.Sprintf("Tried to fail() with unregistered Alert: %s", a.core_alert),
				ERROR_BIT_USED_UNREGISTERED_ALERT)
		}
	}
}

func (self *tracker) safe_start(parent *base_counter) *tracker {
	if self.disabled {
		return self
	}
	if self.parent != parent.parent {
		self.parent.errors.safe_add_error(
			"Tried to start tracking an unregistered Dependency for Objective.",
			ERROR_BIT_USED_UNREGISTERED_DEPENDENCY)
		return disabled_tracker
	}
	tracker := new_tracker(parent)
	self.lock.Lock()
	self.children = append(self.children, tracker)
	self.lock.Unlock()
	return tracker
}

// Predefined Alert message to surface in the event of a failure when fulfilling an Objective or calling a Dependency.
type Alert struct {
	alert *alert
}

func new_Alert(a *alert) Alert {
	if a == nil {
		log.Fatal("a (*alert) cannot be nil")
	}
	return Alert{
		alert: a,
	}
}

// Dependency measures the success rate of calling a dependency and helps trigger alrets in the event of a failure.
type Dependency struct {
	bc *base_counter
}

func new_Dependency(bc *base_counter) Dependency {
	if bc == nil {
		log.Fatal("bc (*base_counter) cannot be nil")
	}
	return Dependency{
		bc: bc,
	}
}

// Statically define alerts (max: 200 chars) expected to be triggered when facing problems with this Dependency.
// Objective and Dependency metadata is automatically added to the Alert and does not need to be included in the
// actual alert message.
func (self Dependency) WithAlert(alert string) Alert {
	if self.bc == nil {
		errors.safe_notify(
			"Invalid API usage detected on Dependency.WithAlert(), monitoring disabled.",
			NOTIFY_BIT_INCORRECT_API_USAGE)
		errors.safe_add_error(
			"Invalid API usage detected on Dependency.WithAlert(), monitoring disabled.",
			ERROR_BIT_INCORRECT_API_USAGE)
		return Alert{}
	}
	a := self.bc.safe_with_alert(alert, 3)
	if a == nil {
		self.bc.errors.safe_add_error("Alert "+alert+" declared twice.",
			ERROR_BIT_USED_ALERT_DOUBLE_REGISTERED)
		return Alert{}
	}
	return new_Alert(a)
}

// Dependency measures the success rate of calling a dependency and helps trigger alrets in the event of a failure.
type DependencyTracker struct {
	tracker *tracker
}

func new_DependencyTracker(t *tracker) DependencyTracker {
	if t == nil {
		log.Fatal("t (*tracker) cannot be nil")
	}
	return DependencyTracker{
		tracker: t,
	}
}

// Success is used to mark the DependencyTracker as successfully completed.
//
// Calling Success() or Fail(...) marks this DependencyTracker as closed.
// The following result in error states that will (1) stop tracking the Objective and (2) Surface an alert on the dashboard when possible:
// - Success() or Fail(...) is invoked again on a closed Tracker.
// - Any tracker is garbage collected without a Success() or Fail(...) call.
func (self DependencyTracker) Success() {
	if self.tracker == nil {
		errors.safe_notify(
			"Invalid API usage detected on DependencyTracker.Success(), monitoring disabled.",
			NOTIFY_BIT_INCORRECT_API_USAGE)
		errors.safe_add_error(
			"Invalid API usage detected on DependencyTracker.Success(), monitoring disabled.",
			ERROR_BIT_INCORRECT_API_USAGE)
		return
	}
	self.tracker.safe_success()
}

// Fail lets nolog know of the failure and the alert to display on the dashboard.
// Error messages (max: 1000 chars) passed to Fail() will be sampled before being sent onwards.
//
// Calling Success() or Fail(...) marks this DependencyTracker as closed.
// The following result in error states that will (1) stop tracking the Objective and (2) Surface an alert on the dashboard when possible:
// - Success() or Fail(...) is invoked again on a closed Tracker.
// - This tracker is garbage collected without a Success() or Fail(...) call.
func (self DependencyTracker) Fail(a Alert, m string) {
	if self.tracker == nil || a.alert == nil {
		errors.safe_notify(
			"Invalid API usage detected on DependencyTracker.Fail(), monitoring disabled.",
			NOTIFY_BIT_INCORRECT_API_USAGE)
		errors.safe_add_error(
			"Invalid API usage detected on DependencyTracker.Fail(), monitoring disabled.",
			ERROR_BIT_INCORRECT_API_USAGE)
		return
	}
	self.tracker.safe_fail(a.alert, m)
}

// ObjectiveTracker tracks a single fulfillment of a given Objective.
type ObjectiveTracker struct {
	tracker *tracker
}

func new_ObjectiveTracker(t *tracker) ObjectiveTracker {
	if t == nil {
		log.Fatal("t (*tracker) cannot be nil")
	}
	return ObjectiveTracker{
		tracker: t,
	}
}

// Success is used to mark the ObjectiveTracker as successfully completed.
//
// Calling Success() or Fail(...) marks this ObjectiveTracker as closed.
// The following result in error states that will (1) stop tracking the Objective and (2) Surface an alert on the dashboard when possible:
// - Success() or Fail(...) is invoked again on a closed Tracker.
// - Any dependencies tracking started as part of this objective aren't already closed via Success() or Fail(...) calls.
// - Any tracker is garbage collected without a Success() or Fail(...) call.
func (self ObjectiveTracker) Success() {
	if self.tracker == nil {
		errors.safe_notify(
			"Invalid API usage detected on ObjectiveTracker.Success(), monitoring disabled.",
			NOTIFY_BIT_INCORRECT_API_USAGE)
		errors.safe_add_error(
			"Invalid API usage detected on ObjectiveTracker.Success(), monitoring disabled.",
			ERROR_BIT_INCORRECT_API_USAGE)
		return
	}
	self.tracker.safe_success()
}

// Fail lets nolog know of the failure and the alert to display on the dashboard.
// Error messages (max: 1000 chars) passed to Fail() will be sampled before being sent onwards.
//
// Calling Success() or Fail(...) marks this ObjectiveTracker as closed.
// The following result in error states that will (1) stop tracking the Objective and (2) Surface an alert on the dashboard when possible:
// - Success() or Fail(...) is invoked again on a closed Tracker.
// - Any dependencies tracking started as part of this objective aren't already closed via Success() or Fail(...) calls.
// - This tracker is garbage collected without a Success() or Fail(...) call.
func (self ObjectiveTracker) Fail(a Alert, m string) {
	if self.tracker == nil || a.alert == nil {
		errors.safe_notify(
			"Invalid API usage detected on ObjectiveTracker.Fail(), monitoring disabled.",
			NOTIFY_BIT_INCORRECT_API_USAGE)
		errors.safe_add_error(
			"Invalid API usage detected on ObjectiveTracker.Fail(), monitoring disabled.",
			ERROR_BIT_INCORRECT_API_USAGE)
		return
	}
	self.tracker.safe_fail(a.alert, m)
}

// Begin tracking the invocation of a dependency.
func (self ObjectiveTracker) StartDependency(d Dependency) DependencyTracker {
	if d.bc == nil {
		errors.safe_notify(
			"Invalid API usage detected on ObjectiveTracker.StartDependency(), monitoring disabled.",
			NOTIFY_BIT_INCORRECT_API_USAGE)
		errors.safe_add_error(
			"Invalid API usage detected on ObjectiveTracker.StartDependency(), monitoring disabled.",
			ERROR_BIT_INCORRECT_API_USAGE)
		return new_DependencyTracker(disabled_tracker)
	}
	return DependencyTracker{
		tracker: self.tracker.safe_start(d.bc),
	}
}

// Objective represents a monitored service goal.
// Each service goal is registered once via a nolog.CreateObjective and an
// Objective is returned to be used in code for monitoring.
//  package example
//
//  import nolog
//
//  var (
//   respondHelloObjective = CreateObjective("RespondHello")
//  )
//
//  func RespondHello(name string) string {
//    respondHello := respondHelloObjective.start()
//    result := "Hello " + name "!"
//    respondHello.success()
//	  return result
//  }
//
// Any dependencies that the Objective relies on and any alerts that may be surfaced should be defined here as well.
//  package example
//
//  import nolog
//
//  var (
//   respondHelloObjective = CreateObjective("RespondHello")
//   unsupportedNameAlert = respondHelloObjective.WithAlert("Unsupported name, starts with A.")
//   lastNameServiceDep = respondHelloObjective.AddDependency("lastNameService")
//  )
//
//  func RespondHello(name string) (string, error) {
//    respondHello := respondHelloObjective.start()
//    if name.startsWith("a") || name.startsWith("A") {
//	    respondHello.Fail(unsupportedNameAlert, name)
//      return "", fmt.Errorf("Unsupported name %s", name)
//    }
//    callLastName := lastNameServiceDep.start()
//    lastName := getLastName(name)
//    callLastName.success()
//    result := "Hello " + name +" " + lastName + "!"
//    respondHello.success()
//	  return result, nil
//  }
type Objective struct {
	bc *base_counter
}

func new_Objective(bc *base_counter) Objective {
	if bc == nil {
		log.Fatal("bc (*base_counter) cannot be nil")
	}
	return Objective{
		bc: bc,
	}
}

// Add a Dependency to this Objective used for tracking.
//  dependency: 	the name of the dependency (max: 40 chars)
//  action: 		a description of the the feature of the dependency being relied on. (max: 40 chars)
//
// The three recommended approaches for action are to either:
//
// 1. Pass in either a simple-descriptor for the work being performed by the dependency
//  AddDependency("AccountAPI", "GetOwnerFromAccount")
//
// 2. the descriptive HTTP path exposed by the dependency.
//  AddDependency("AccountAPI", "/get-account")
//
// 3. in the event of a database, the the table name being relied on.
//  AddDependency("MySqlDatabase", "Accounts")
func (self Objective) AddDependency(name, action string) Dependency {
	if self.bc == nil {
		errors.safe_notify(
			"Invalid API usage detected on Objective.Start(), monitoring disabled.",
			NOTIFY_BIT_INCORRECT_API_USAGE)
		errors.safe_add_error(
			"Invalid API usage detected on Objective.Start(), monitoring disabled.",
			ERROR_BIT_INCORRECT_API_USAGE)
		return Dependency{}
	}
	name = trim(name, 40, true);
	action = trim(action, 40, true);
	bc := self.bc.safe_add_child(name, action, 6)
	return new_Dependency(bc)
}

// Statically define alerts (max: 200 chars) expected to be triggered when facing problems fulfilling this Objective.
// Objective metadata is automatically added to the Alert and does not need to be included in the
// actual alert message.
func (self Objective) WithAlert(alert string) Alert {
	if self.bc == nil {
		errors.safe_notify(
			"Invalid API usage detected on Objective.Start(), monitoring disabled.",
			NOTIFY_BIT_INCORRECT_API_USAGE)
		errors.safe_add_error(
			"Invalid API usage detected on Objective.Start(), monitoring disabled.",
			ERROR_BIT_INCORRECT_API_USAGE)
		return Alert{}
	}
	a := self.bc.safe_with_alert(alert, 3)
	if a == nil {
		self.bc.errors.safe_add_error("Alert "+alert+" declared twice.",
			ERROR_BIT_USED_ALERT_DOUBLE_REGISTERED)
		return Alert{}
	}
	return new_Alert(a)
}

// Start tracking the fulfillment of this objective.
func (self Objective) Start() ObjectiveTracker {
	if self.bc == nil {
		errors.safe_notify(
			"Invalid API usage detected on Objective.Start(), monitoring disabled.",
			NOTIFY_BIT_INCORRECT_API_USAGE)
		errors.safe_add_error(
			"Invalid API usage detected on Objective.Start(), monitoring disabled.",
			ERROR_BIT_INCORRECT_API_USAGE)
		return new_ObjectiveTracker(disabled_tracker)
	}
	return new_ObjectiveTracker(self.bc.safe_start())
}

type env_bit int8

const (
	ENV_BIT_STD env_bit = iota
	ENV_BIT_LOCAL
	ENV_BIT_PERFORMANCE
	ENV_BIT_PRODUCTION
)

const reserved_block_name = "NoLogDefault"

var (
	mutex       = sync.Mutex{}
	initialized = false
	objectives  = map[string]Objective{}
	errors      = new_error_handler()
	service_id  = ""
	instance_id = ""
	version_id  = ""
	raw_key     = ""
	env         = ENV_BIT_STD
	target      = ""
	shard       = "0"
	shard_token = "init"
)

func unsafe_create_base_proto() *writespec.WriteRequest {
	return &writespec.WriteRequest{
		Ids: &writespec.ServiceInstanceIdentifier{
			ServiceId:  service_id,
			InstanceId: instance_id,
			VersionId:  version_id,
		},
		CreationTimestamp: &writespec.Timestamp{
			UtcCreationTime: time.Now().Unix(),
		},
		RawInformation: &writespec.WriteRequest_RawInformation{
			Blocks: make([]*writespec.WriteRequest_RawInformation_CriticalBlock, 0, len(objectives)),
		},
	}
}

func __report() {
	if errors.safe_has_error() {
		return
	}
	mutex.Lock()
	w := unsafe_create_base_proto()
	objs := make([]Objective, 0, len(objectives))
	for _, obj := range objectives {
		objs = append(objs, obj)
	}
	mutex.Unlock()
	for _, obj := range objs {
		obj_name, _, obj_is_successful, obj_successful, obj_failing, obj_children, obj_alerts := obj.bc.proto_data()
		state := writespec.State_STATE_ALL_OK
		if !obj_is_successful {
			state = writespec.State_STATE_CRITICAL_FAILURE
		}
		critical_block := writespec.WriteRequest_RawInformation_CriticalBlock{
			Name: obj_name,
			Counts: &writespec.WriteRequest_RawInformation_Counts{
				Success: int32(obj_successful),
				Failed:  int32(obj_failing),
			},
			BlockDependency: []*writespec.WriteRequest_RawInformation_CriticalBlock_BlockDependency{},
			State:           state,
			Alerts:          []*writespec.AlertInformation{},
		}
		w.RawInformation.Blocks = append(w.RawInformation.Blocks, &critical_block)
		obj_error_bits, obj_errors := obj.bc.errors.safe_get_errors()
		if obj_error_bits > 0 {
			critical_block.Alerts = append(critical_block.Alerts, &writespec.AlertInformation{
				CoreAlert:      "Objective misconfigured.",
				SampledContent: []string{},
				TotalCount:     int64(len(obj_errors)),
				Timestamp:      time.Now().Unix(),
			})
			for _, err := range obj_errors {
				critical_block.Alerts[0].SampledContent = append(critical_block.Alerts[0].SampledContent, err)
				if len(critical_block.Alerts[0].SampledContent) == 3 {
					break
				}
			}
		} else {
			for _, dep := range obj_children {
				dep_name, dep_action, dep_is_successful, dep_successful, dep_failing, _, dep_alerts := dep.proto_data()
				dep_state := writespec.State_STATE_ALL_OK
				if !dep_is_successful {
					dep_state = writespec.State_STATE_CRITICAL_FAILURE
					if critical_block.State != writespec.State_STATE_ALL_OK {
						critical_block.State = writespec.State_STATE_DEPENDENCY_FAILURE
					}
				}
				blockDep := writespec.WriteRequest_RawInformation_CriticalBlock_BlockDependency{
					Dependency: dep_name,
					Action:     dep_action,
					Counts: &writespec.WriteRequest_RawInformation_Counts{
						Success: int32(dep_successful),
						Failed:  int32(dep_failing),
					},
					State:  dep_state,
					Alerts: []*writespec.AlertInformation{},
				}
				for _, alert := range dep_alerts {
					needs_reporting, samples, count, timestamp := alert.safe_sample_alerts()
					if !needs_reporting {
						continue
					}
					blockDep.Alerts = append(blockDep.Alerts, &writespec.AlertInformation{
						Timestamp:      timestamp,
						CoreAlert:      alert.core_alert,
						SampledContent: samples,
						TotalCount:     count,
					})
				}
				critical_block.BlockDependency = append(critical_block.BlockDependency, &blockDep)
			}
			for _, alert := range obj_alerts {
				needs_reporting, samples, count, timestamp := alert.safe_sample_alerts()
				if !needs_reporting {
					continue
				}
				critical_block.Alerts = append(critical_block.Alerts, &writespec.AlertInformation{
					Timestamp:      timestamp,
					CoreAlert:      alert.core_alert,
					SampledContent: samples,
					TotalCount:     count,
				})
			}
		}
	}
	if len(w.RawInformation.Blocks) == 0 {
		w.RawInformation.Blocks = append(w.RawInformation.Blocks, &writespec.WriteRequest_RawInformation_CriticalBlock{
			Name:            reserved_block_name,
			BlockDependency: []*writespec.WriteRequest_RawInformation_CriticalBlock_BlockDependency{},
			State:           writespec.State_STATE_NO_DATA,
		})
	}
	write_report_with_retry(w, 0)
}

type retry_status_code int8

const (
	RETRY_STATUS_ALL_GOOD retry_status_code = iota
	RETRY_STATUS_RETRY
	RETRY_STATUS_BAD
)

var (
	lbs = []string{"alpha", "omega"}
)

func write_report_with_retry(w *writespec.WriteRequest, attempt int) {
	if attempt >= 2 {
		return
	}
	if w == nil {
		log.Fatal("This should never happen, tried to write_report_with_retry with nil data.")
	}
	if write_report(w, lbs[attempt%len(lbs)]) == RETRY_STATUS_RETRY { // Predefined Alert message to surface in the event of a failure when fulfilling an Objective or calling a Dependency.
		r, err := rand.Int(rand.Reader, big.NewInt(2000))
		rr := r.Int64()
		if err != nil {
			rr = 1000
		}
		time.Sleep(time.Duration(rr) * time.Millisecond)
		write_report_with_retry(w, attempt+1)
	}
}

var (
	httpClient = &http.Client{
		Timeout: time.Second * 4,
	}
)

func write_report(w *writespec.WriteRequest, lb string) retry_status_code {
	if env == ENV_BIT_STD {
		log.Printf("STD-ENV: %v", w)
		return RETRY_STATUS_ALL_GOOD
	}
	w.ShardInfo = &writespec.WriteRequest_ShardInfo{
		ShardToken: shard_token,
	}
	upbytes, err := proto.Marshal(w)
	if err != nil {
		log.Fatal("Marshalling write request should always succeed, but found error: ", err)
	}
	pbytesBuf := bytes.NewBuffer([]byte{})
	gz := gzip.NewWriter(pbytesBuf)
	_, err = gz.Write(upbytes)
	if err != nil {
		log.Fatal("gzip.NewWriter.Write should never fail, but found error: ", err)
	}
	err = gz.Close()
	if err != nil {
		log.Fatal("gzip.NewWriter.Write should never fail, but found error: ", err)
	}
	url := target
	if env == ENV_BIT_PERFORMANCE {
		url = fmt.Sprintf(url, shard)
	} else if env == ENV_BIT_PRODUCTION {
		url = fmt.Sprintf(url, lb, shard)
	}
	url = url + "/be/write"
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(pbytesBuf.Bytes()))
	if err != nil {
		log.Fatal("http.NewRequest should never fail, but found error: ", err)
	}
	basic_token := "Basic " + raw_key

	req.Header.Set("Authorization", basic_token)
	response, err := httpClient.Do(req)
	if err != nil {
		return RETRY_STATUS_RETRY
	}
	defer response.Body.Close()

	response_bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return RETRY_STATUS_RETRY
	}
	response_proto := &writespec.WriteResponse{}
	err = proto.Unmarshal(response_bytes, response_proto)
	if err != nil {
		return RETRY_STATUS_RETRY
	}
	// Don't retry for predefined error codes
	if response_proto != nil && response_proto.WriteStatus != nil &&
		(response_proto.WriteStatus.StatusCode == writespec.WriteResponseCode_FAILURE_APIKEY_CANNOT_DECRYPT ||
			response_proto.WriteStatus.StatusCode == writespec.WriteResponseCode_FAILURE_APIKEY_CANNOT_UNBASE64 ||
			response_proto.WriteStatus.StatusCode == writespec.WriteResponseCode_FAILURE_APIKEY_CANNOT_UNMARSHAL ||
			response_proto.WriteStatus.StatusCode == writespec.WriteResponseCode_FAILURE_APIKEY_EXPIRED ||
			response_proto.WriteStatus.StatusCode == writespec.WriteResponseCode_FAILURE_APIKEY_MISSING_FIELD ||
			response_proto.WriteStatus.StatusCode == writespec.WriteResponseCode_FAILURE_APIKEY_INVALID) {
		return RETRY_STATUS_BAD
	}

	if response.StatusCode != 200 {
		return RETRY_STATUS_RETRY
	}

	if response_proto.Shardstate != nil {
		if response_proto.GetOk() {
			return RETRY_STATUS_ALL_GOOD
		}
		if response_proto.GetUpdateShard() != nil {
			new_shard := response_proto.GetUpdateShard().Shard
			new_token := response_proto.GetUpdateShard().ShardToken
			num_only := true
			for _, c := range new_shard {
				if !unicode.IsDigit(c) {
					num_only = false
					break
				}
			}
			if num_only {
				shard = new_shard
				shard_token = new_token
			} else {
				shard = "0"
				shard_token = "gononnumeric"
			}
		}
	}
	return RETRY_STATUS_RETRY
}

// CreateObjective returns an Objective (max: 40 chars) used for monitoring a Service objective.
// This should be called once to register the Objective:
//  package example
//
//  import nolog
//
//  var (
//   respondHelloObjective = CreateObjective("RespondHello")
//  )
//
//  func RespondHello(name string) string {
//    respondHello := respondHelloObjective.start()
//    result := "Hello " + name + "!"
//    respondHello.success()
//	  return result
//  }
func CreateObjective(name string) Objective {
	name = trim(name, 40, true)
	if strings.HasPrefix(name, "nolog") {
		name = "custom." + name
	}
	mutex.Lock()
	obj, ok := objectives[name]
	if ok {
		obj.bc.errors.safe_add_error(
			"Called CreateObjective multiple times with same 'name'. Disabling tracking of Objective.",
			ERROR_BIT_DUPLICATE_OBJECTIVE)
	} else {
		obj = new_Objective(new_base_counter(name, "", initialized, 6, nil))
		objectives[name] = obj
	}
	mutex.Unlock()
	return obj
}

// Initialize needs to be called in main() before the program begins monitoring with NoLog.
//
//  serviceID:       The name of the service being monitored shared across instances. (max: 40 chars)
//  instanceID:      A unique instance identifier to diffrentiate instances. (max: 40 chars)
//  versionID:       Version ID of the software running, used to differentiate instances during partial deployment. (max: 10 chars)
//  noLogConfig:     NoLog API Key downloaded from nolog.io
func Initialize(serviceId, instanceId, versionId, noLogConfig string) {
	if len(noLogConfig) > 5000 {
		noLogConfig = noLogConfig[0:5000]
	}
	mutex.Lock()
	defer mutex.Unlock()
	if initialized {
		errors.safe_add_error(
			"nolog.Initialize() should only be called once.",
			ERROR_BIT_MULTIPLE_INITIALIZATION)
		return
	}
	if len(noLogConfig) == 0 {
		errors.safe_notify(
			"Cannot initialize NoLog with empty key",
			NOTIFY_BIT_EMPTY_API_KEY)
		errors.safe_add_error(
			"Cannot initialize NoLog with empty key",
			ERROR_BIT_EMPTY_API_KEY)
	}
	if len(serviceId) == 0 {
		errors.safe_notify(
			"ServiceID '' is missing",
			NOTIFY_BIT_SERVICE_ID_MISSING)
		return
	}
	if len(instanceId) == 0 {
		errors.safe_notify(
			"InstanceID '' is missing",
			NOTIFY_BIT_INSTANCE_ID_MISSING)
		return
	}
	service_id = trim(serviceId, 40, true)
	instance_id = trim(instanceId, 40, true)
	version_id = trim(versionId, 10, true)
	initialized = true
	for _, obj := range objectives {
		obj.bc.safe_update_init(initialized)
	}
	raw_key = noLogConfig
	key := auth.ClientKey{}
	if noLogConfig != "local" {
		decodedConfig, err := base64.StdEncoding.DecodeString(noLogConfig)
		if err != nil {
			errors.safe_notify(
				"Cannot initialize NoLog with invalid API key",
				NOTIFY_BIT_INVALID_API_KEY)
			errors.safe_add_error(
				"Cannot initialize NoLog with invalid API key",
				ERROR_BIT_INVALID_API_KEY)
			return
		}
		err = proto.Unmarshal(decodedConfig, &key)
		if err != nil {
			errors.safe_notify(
				"Cannot initialize NoLog with invalid API key",
				NOTIFY_BIT_INVALID_API_KEY)
			errors.safe_add_error(
				"Cannot initialize NoLog with invalid API key",
				ERROR_BIT_INVALID_API_KEY)
			return
		}
		envs := map[string]env_bit{
			"LOCAL":       ENV_BIT_LOCAL,
			"PERFORMANCE": ENV_BIT_PERFORMANCE,
			"PRODUCTION":  ENV_BIT_PRODUCTION,
		}
		targets := map[env_bit]string{
			ENV_BIT_LOCAL:       "http://localhost:8080",
			ENV_BIT_PERFORMANCE: "https://performance.nolog.io/writer%s",
			ENV_BIT_PRODUCTION:  "https://%s.nolog.io/writer%s"}
		env_found := false
		for key, value := range key.KeyFields {
			if key == "n" {
				env = envs[value]
				target = targets[env]
				env_found = true
				break
			}
		}
		if !env_found {
			errors.safe_notify(
				"Cannot initialize NoLog with invalid API key",
				NOTIFY_BIT_INVALID_API_KEY)
			errors.safe_add_error(
				"Cannot initialize NoLog with invalid API key",
				ERROR_BIT_INVALID_API_KEY)
			return
		}
	}
	go func() {
		r, err := rand.Int(rand.Reader, big.NewInt(2000))
		rr := r.Int64()
		if err != nil {
			rr = 1000
		}
		nextExec := time.Now().Add(time.Millisecond * time.Duration(rr))
		nextExec = nextExec.Add(time.Second * 10)
		for {
			<-time.After(time.Until(nextExec))
			nextExec = time.Now().Add(10 * time.Second)
			__report()
		}
	}()
}
