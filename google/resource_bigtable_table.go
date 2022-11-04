package google

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/structure"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func resourceBigtableTable() *schema.Resource {
	return &schema.Resource{
		Create:        resourceBigtableTableCreate,
		Read:          resourceBigtableTableRead,
		Update:        resourceBigtableTableUpdate,
		Delete:        resourceBigtableTableDestroy,
		CustomizeDiff: resourceBigtableFamilyGCPolicyCustomizeDiff,

		Importer: &schema.ResourceImporter{
			State: resourceBigtableTableImport,
		},

		// Set a longer timeout for table creation as adding column families can be slow.
		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(45 * time.Minute),
		},

		// ----------------------------------------------------------------------
		// IMPORTANT: Do not add any additional ForceNew fields to this resource.
		// Destroying/recreating tables can lead to data loss for users.
		// ----------------------------------------------------------------------
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    false,
				Description: `The name of the table.`,
			},

			"column_family": {
				Type:        schema.TypeSet,
				Optional:    true,
				ForceNew:    false,
				Description: `A group of columns within a table which share a common configuration. This can be specified multiple times.`,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"family": {
							Type:        schema.TypeString,
							Required:    true,
							ForceNew:    false,
							Description: `The name of the column family.`,
						},

						// TF diff fixes requires  terraform-plugin-framework migration, but it works as intended now, there is only a cosmetic quirk
						// https://discuss.hashicorp.com/t/typeset-is-not-picking-up-correct-changes/23735/4
						"gc_policy": {
							Type:        schema.TypeSet,
							Optional:    true,
							ForceNew:    false,
							MaxItems:    1,
							Description: `The garbage collection rule which deletes cells in a column older than the given age.`,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"gc_rules": {
										Type:         schema.TypeString,
										Optional:     true,
										ForceNew:     false,
										Description:  `Serialized JSON string for garbage collection policy. Conflicts with "mode", "max_age" and "max_version".`,
										ValidateFunc: validation.StringIsJSON,
										// ConflictsWith: []string{"mode", "max_age", "max_version"},
										StateFunc: func(v interface{}) string {
											json, _ := structure.NormalizeJsonString(v)
											return json
										},
									},
									"mode": {
										Type:         schema.TypeString,
										Optional:     true,
										ForceNew:     false,
										Description:  `If multiple policies are set, you should choose between UNION OR INTERSECTION.`,
										ValidateFunc: validation.StringInSlice([]string{GCPolicyModeIntersection, GCPolicyModeUnion}, false),
										// ConflictsWith: []string{"column_family.#.gc_policy.gc_rules"},
									},
									"max_age": {
										Type:        schema.TypeSet,
										Optional:    true,
										ForceNew:    false,
										Description: `GC policy that applies to all cells older than the given age.`,
										MaxItems:    1,
										Elem: &schema.Resource{
											Schema: map[string]*schema.Schema{
												"days": {
													Type:        schema.TypeInt,
													Optional:    true,
													Computed:    true,
													ForceNew:    false,
													Deprecated:  "Deprecated in favor of duration",
													Description: `Number of days before applying GC policy.`,
													// ExactlyOneOf: []string{"max_age.0.days", "max_age.0.duration"},
												},
												"duration": {
													Type:         schema.TypeString,
													Optional:     true,
													Computed:     true,
													ForceNew:     false,
													Description:  `Duration before applying GC policy`,
													ValidateFunc: validateDuration(),
													// ExactlyOneOf: []string{"max_age.0.days", "max_age.0.duration"},
												},
											},
										},
										// ConflictsWith: []string{"column_family.#.gc_policy.gc_rules"},
									},
									"max_version": {
										Type:        schema.TypeSet,
										Optional:    true,
										ForceNew:    false,
										Description: `GC policy that applies to all versions of a cell except for the most recent.`,
										Elem: &schema.Resource{
											Schema: map[string]*schema.Schema{
												"number": {
													Type:        schema.TypeInt,
													Required:    true,
													ForceNew:    false,
													Description: `Number of version before applying the GC policy.`,
												},
											},
										},
										// ConflictsWith: []string{"column_family.#.gc_policy.gc_rules"},
									},
								},
							},
						},
					},
				},
			},

			"instance_name": {
				Type:             schema.TypeString,
				Required:         true,
				ForceNew:         true,
				DiffSuppressFunc: compareResourceNames,
				Description:      `The name of the Bigtable instance.`,
			},

			"split_keys": {
				Type:        schema.TypeList,
				Optional:    true,
				ForceNew:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Description: `A list of predefined keys to split the table on. !> Warning: Modifying the split_keys of an existing table will cause Terraform to delete/recreate the entire google_bigtable_table resource.`,
			},

			"project": {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				ForceNew:    true,
				Description: `The ID of the project in which the resource belongs. If it is not provided, the provider project is used.`,
			},
		},
		UseJSONNumber: true,
	}
}

func resourceBigtableTableCreate(d *schema.ResourceData, meta interface{}) error {
	fmt.Println(">>>>>>>>>>>>>> dGetRawConfig :", d.GetRawConfig())
	config := meta.(*Config)
	userAgent, err := generateUserAgentString(d, config.userAgent)
	if err != nil {
		return err
	}

	ctx := context.Background()

	project, err := getProject(d, config)
	if err != nil {
		return err
	}

	instanceName := GetResourceNameFromSelfLink(d.Get("instance_name").(string))
	c, err := config.BigTableClientFactory(userAgent).NewAdminClient(project, instanceName)
	if err != nil {
		return fmt.Errorf("Error starting admin client. %s", err)
	}
	if err := d.Set("instance_name", instanceName); err != nil {
		return fmt.Errorf("Error setting instance_name: %s", err)
	}

	defer c.Close()

	tableId := d.Get("name").(string)
	tblConf := bigtable.TableConf{TableID: tableId}

	// Set the split keys if given.
	if v, ok := d.GetOk("split_keys"); ok {
		tblConf.SplitKeys = convertStringArr(v.([]interface{}))
	}

	// Set the column families if given.
	columnFamilies := make(map[string]bigtable.GCPolicy)
	fmt.Println(">>>>>>>>>>>>>>>>>> TypeOf columnFamilies :", fmt.Sprintf("%T", columnFamilies))
	if d.Get("column_family.#").(int) > 0 {
		columns := d.Get("column_family").(*schema.Set).List()
		// columns := d.Get("column_family").(([]interface{}))

		for _, co := range columns {
			column := co.(map[string]interface{})

			gc_policy, ok := column["gc_policy"]

			if !ok {
				fmt.Println("gc_policy1:", gc_policy)
				// baz was not of type *foo. The assertion failed
			}

			gcPolicy, err := generateBigtableFamilyGCPolicy(gc_policy.(*schema.Set).List()[0].(map[string]interface{}))
			// gcPolicy, err := generateBigtableFamilyGCPolicy(gc_policy.([]interface{})[0].(map[string]interface{}))
			if err != nil {
				return err
			}

			if v, ok := column["family"]; ok {
				// By default, there is no GC rules.
				// columnFamilies[v.(string)] = bigtable.NoGcPolicy()

				// Here we can add the GC rules to avoid having them separated
				columnFamilies[v.(string)] = gcPolicy

			}
		}
	}
	tblConf.Families = columnFamilies

	// This method may return before the table's creation is complete - we may need to wait until
	// it exists in the future.
	// Set a longer timeout as creating table and adding column families can be pretty slow.
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel() // Always call cancel.
	err = c.CreateTableFromConf(ctxWithTimeout, &tblConf)
	if err != nil {
		return fmt.Errorf("Error creating table. %s", err)
	}

	id, err := replaceVars(d, config, "projects/{{project}}/instances/{{instance_name}}/tables/{{name}}")
	if err != nil {
		return fmt.Errorf("Error constructing id: %s", err)
	}
	d.SetId(id)

	return resourceBigtableTableRead(d, meta)
}

func resourceBigtableTableRead(d *schema.ResourceData, meta interface{}) error {
	config := meta.(*Config)
	userAgent, err := generateUserAgentString(d, config.userAgent)
	if err != nil {
		return err
	}
	ctx := context.Background()

	project, err := getProject(d, config)
	if err != nil {
		return err
	}

	instanceName := GetResourceNameFromSelfLink(d.Get("instance_name").(string))
	c, err := config.BigTableClientFactory(userAgent).NewAdminClient(project, instanceName)
	if err != nil {
		return fmt.Errorf("Error starting admin client. %s", err)
	}

	defer c.Close()

	name := d.Get("name").(string)
	table, err := c.TableInfo(ctx, name)

	if err != nil {
		log.Printf("[WARN] Removing %s because it's gone", name)
		d.SetId("")
		return nil
	}

	if err := d.Set("project", project); err != nil {
		return fmt.Errorf("Error setting project: %s", err)
	}

	if err := d.Set("column_family", flattenColumnFamily(table.FamilyInfos)); err != nil {
		return fmt.Errorf("Error setting column_family: %s", err)
	}

	return nil
}

func resourceBigtableTableUpdate(d *schema.ResourceData, meta interface{}) error {
	fmt.Printf(">>>22resourceBigtableTableUpdate : %#v", d)
	config := meta.(*Config)
	userAgent, err := generateUserAgentString(d, config.userAgent)
	if err != nil {
		return err
	}
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	project, err := getProject(d, config)
	if err != nil {
		return err
	}

	instanceName := GetResourceNameFromSelfLink(d.Get("instance_name").(string))
	c, err := config.BigTableClientFactory(userAgent).NewAdminClient(project, instanceName)
	if err != nil {
		return fmt.Errorf("Error starting admin client. %s", err)
	}
	defer c.Close()

	o, n := d.GetChange("column_family")
	// oSet := o.([]interface{})
	oSet := o.(*schema.Set).List()

	// nSet := n.([]interface{})
	nSet := n.(*schema.Set).List()

	deletedCF := getDeletedColumnFamilies(oSet, nSet)
	fmt.Printf("deletedCF: %#v", deletedCF)

	createdCF, updatedCF := getUpdatedAndCreatedColumnFamilies(oSet, nSet)

	// Bigtable table name
	name := d.Get("name").(string)

	// Remove deleted column families
	for _, cf := range deletedCF {
		cfname := getCFName(cf)
		if err := c.DeleteColumnFamily(ctx, name, cfname); err != nil {
			return fmt.Errorf("Error deleting column family %q: %s", cfname, err)
		}
	}

	// Create new column families
	for _, cf := range createdCF {
		cfname := getCFName(cf)
		if err := c.CreateColumnFamily(ctx, name, cfname); err != nil {
			return fmt.Errorf("Error creating column family %q: %s", cfname, err)
		}

		gcpolicy, _ := getCFGCPolicy(cf)
		fmt.Printf("Creating Policy for new CF %s:  %#v", cfname, gcpolicy)
		if err := c.SetGCPolicy(ctx, name, cfname, gcpolicy); err != nil {
			return fmt.Errorf("Error setting GCPolicy for column family %q: %s", cfname, err)
		}
	}

	// Update column families (gc policy update)
	for _, cf := range updatedCF {
		cfname := getCFName(cf)

		gcpolicy, _ := getCFGCPolicy(cf)
		if err := c.SetGCPolicy(ctx, name, cfname, gcpolicy); err != nil {
			return fmt.Errorf("Error setting GCPolicy for column family %q: %s", cfname, err)
		}

	}

	return resourceBigtableTableRead(d, meta)
}

func getCFName(cf interface{}) string {
	return cf.(map[string]interface{})["family"].(string)
}

func getCFGCPolicy(cf interface{}) (bigtable.GCPolicy, error) {
	gc_policy := cf.(map[string]interface{})["gc_policy"]
	policyMap := gc_policy.(*schema.Set).List()[0].(map[string]interface{})
	// policyMap := gc_policy.([]interface{})[0].(map[string]interface{})
	gcpolicy, err := generateBigtableFamilyGCPolicy(policyMap)

	if err != nil {
		cfname := getCFName(cf)
		return nil, fmt.Errorf("Error generating GC policy for column family %q: %s", cfname, err)
	}
	return gcpolicy, nil
}

func getDeletedColumnFamilies(oldCF []interface{}, newCF []interface{}) []interface{} {
	deleted := make([]interface{}, 0, len(oldCF))

	for _, old := range oldCF {
		oldFam := old.(map[string]interface{})["family"]
		fmt.Println("oldFAM :", old.(map[string]interface{})["family"])

		found := false
		for _, new := range newCF {
			newFam := new.(map[string]interface{})["family"]
			fmt.Println("newFAM :", new.(map[string]interface{})["family"])

			if newFam == oldFam {
				found = true
				break
			}
		}

		if !found {
			deleted = append(deleted, old)
		}
	}

	return deleted
}

func getUpdatedAndCreatedColumnFamilies(oldCF []interface{}, newCF []interface{}) ([]interface{}, []interface{}) {
	updated := make([]interface{}, 0, len(newCF))
	created := make([]interface{}, 0, len(newCF))

	for _, new := range newCF {
		fmt.Printf(".....new : %#v", new)
		newFam := new.(map[string]interface{})["family"]

		if newFam == "" {
			continue
		}
		fmt.Println("newFam :", new.(map[string]interface{})["family"])

		found := false
		isUpdated := false
		for _, old := range oldCF {
			fmt.Printf(".....old : %#v", new)

			oldFam := old.(map[string]interface{})["family"]
			fmt.Println("oldFam :", old.(map[string]interface{})["family"])

			if newFam == oldFam {
				equal := reflect.DeepEqual(old, new)
				found = true
				if !equal {
					fmt.Printf("Is not EQUAL OLD! %#v", old)
					fmt.Printf("Is not EQUAL NEW! %#v", new)
					isUpdated = true
				}
				break
			}
		}

		if found && isUpdated {
			updated = append(updated, new)
		}

		if !found {
			created = append(created, new)
		}
	}

	return created, updated
}

func resourceBigtableTableDestroy(d *schema.ResourceData, meta interface{}) error {
	config := meta.(*Config)
	userAgent, err := generateUserAgentString(d, config.userAgent)
	if err != nil {
		return err
	}

	ctx := context.Background()

	project, err := getProject(d, config)
	if err != nil {
		return err
	}

	instanceName := GetResourceNameFromSelfLink(d.Get("instance_name").(string))
	c, err := config.BigTableClientFactory(userAgent).NewAdminClient(project, instanceName)
	if err != nil {
		return fmt.Errorf("Error starting admin client. %s", err)
	}

	defer c.Close()

	name := d.Get("name").(string)
	err = c.DeleteTable(ctx, name)
	if err != nil {
		return fmt.Errorf("Error deleting table. %s", err)
	}

	d.SetId("")

	return nil
}

func flattenColumnFamily(families []bigtable.FamilyInfo) []map[string]interface{} {
	result := make([]map[string]interface{}, 0, len(families))

	for _, v := range families {
		data := make(map[string]interface{})
		data["family"] = v.Name
		data["gc_policy"], _ = flattenGCRule(v.FullGCPolicy)
		result = append(result, data)
	}

	return result
}

func addFirst(s []int, insertValue int) []int {
	res := make([]int, len(s)+1)
	copy(res[1:], s)
	res[0] = insertValue
	return res
}

func flattenGCRule(rule bigtable.GCPolicy) ([]map[string]interface{}, error) {
	gcs, err := gcPolicyToGCRuleString(rule, false)
	if err != nil {
		return nil, err
	}

	result := make([]map[string]interface{}, 0, 1)

	data := make(map[string]interface{})

	// Default values
	data["mode"] = ""
	data["gc_rules"] = ""

	if v, ok := gcs["mode"]; ok {
		data["mode"] = v
	}

	if v, ok := gcs["max_version"]; ok {
		data["max_version"] = []interface{}{map[string]interface{}{"number": v}}
		// data["max_version"] = []interface{}{map[string]interface{}{"number": v}}
	}

	if v, ok := gcs["max_age"]; ok {
		data["max_age"] = []interface{}{map[string]interface{}{"duration": v}}
		// data["max_age"] = []interface{}{map[string]interface{}{"duration": v}}
	}

	maxAge := gcs["max_age"]
	maxVersion := gcs["max_version"]

	if gcs["mode"] == "" && len(maxAge.([]interface{})) == 0 && len(maxVersion.([]interface{})) == 0 {
		gcRuleString, err := gcPolicyToGCRuleString(rule, true)
		if err != nil {
			return nil, err
		}
		gcRuleJsonString, err := json.Marshal(gcRuleString)
		if err != nil {
			return nil, fmt.Errorf("Error marshaling GC policy to json: %s", err)
		}
		data["gc_rules"] = string(gcRuleJsonString)
	}

	data["mode"] = gcs["mode"]
	result = append(result, data)

	return result, nil
}

// TODO(rileykarson): Fix the stored import format after rebasing 3.0.0
func resourceBigtableTableImport(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	config := meta.(*Config)
	if err := parseImportId([]string{
		"projects/(?P<project>[^/]+)/instances/(?P<instance_name>[^/]+)/tables/(?P<name>[^/]+)",
		"(?P<project>[^/]+)/(?P<instance_name>[^/]+)/(?P<name>[^/]+)",
		"(?P<instance_name>[^/]+)/(?P<name>[^/]+)",
	}, d, config); err != nil {
		return nil, err
	}

	// Replace import id for the resource id
	id, err := replaceVars(d, config, "projects/{{project}}/instances/{{instance_name}}/tables/{{name}}")
	if err != nil {
		return nil, fmt.Errorf("Error constructing id: %s", err)
	}
	d.SetId(id)

	return []*schema.ResourceData{d}, nil
}

func resourceBigtableFamilyGCPolicyCustomizeDiffFunc(diff TerraformResourceDiff) error {
	// hasChange := diff.HasChange("column_family")
	// columnFamilies := diff.Get("column_family")
	old, new := diff.GetChange("column_family")

	log.Printf("old :%#v", old)
	log.Printf("new :%#v", new)

	// Comparing the old and new duration values for each column family
	// for idx, oldCF := range old.([]interface{}) {
	for idx, oldCF := range old.(*schema.Set).List() {
		fmt.Println("idx1 :", idx)

		oldCFMap := oldCF.(map[string]interface{})
		oldCFName := oldCFMap["family"].(string)

		log.Printf("oldCFMap :%#v", oldCFMap)

		// oldMaxAgeRules := oldCFMap["gc_policy"].([]interface{})[0].(map[string]interface{})["max_age"].([]interface{})
		oldMaxAgeRules := oldCFMap["gc_policy"].(*schema.Set).List()[0].(map[string]interface{})["max_age"].(*schema.Set).List()
		// oldMaxAgeRules := oldCFMap["gc_policy"].(*schema.Set).List()[0].(map[string]interface{})["max_age"].([]interface{})
		// oldMaxAgeRules := oldCFMap["gc_policy"].(*schema.Set).List()[0].(map[string]interface{})["max_age"].(*schema.Set).List()

		if len(oldMaxAgeRules) > 0 {

			oldCFDuration := oldMaxAgeRules[0].(map[string]interface{})["duration"].(string)
			oldCFDurationTime, _ := getMaxAgeDuration2(oldCFDuration)

			// for _, newCF := range new.([]interface{}) {
			for _, newCF := range new.(*schema.Set).List() {
				fmt.Printf("newCF : %#v", newCF)
				newCFMap := newCF.(map[string]interface{})
				newCFName := newCFMap["family"].(string)

				// newMaxAgeRules := newCFMap["gc_policy"].([]interface{})[0].(map[string]interface{})["max_age"].([]interface{})
				// newMaxAgeRules := newCFMap["gc_policy"].(*schema.Set).List()[0].(map[string]interface{})["max_age"].([]interface{})
				newMaxAgeRules := newCFMap["gc_policy"].(*schema.Set).List()[0].(map[string]interface{})["max_age"].(*schema.Set).List()
				if len(newMaxAgeRules) > 0 {

					newCFDuration := newMaxAgeRules[0].(map[string]interface{})["duration"].(string)
					newCFDurationTime, _ := getMaxAgeDuration2(newCFDuration)

					if oldCFName == newCFName && oldCFDurationTime == newCFDurationTime {
						// TODO: Fix this (doesn't work with TypeSet)
						diffUpdate := fmt.Sprintf("column_family.%d.gc_policy.0.max_age.0.duration", idx)
						err := diff.Clear(diffUpdate)
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}

	return nil
}

func resourceBigtableFamilyGCPolicyCustomizeDiff(_ context.Context, d *schema.ResourceDiff, meta interface{}) error {
	return resourceBigtableFamilyGCPolicyCustomizeDiffFunc(d)
}

func generateBigtableFamilyGCPolicy(d map[string]interface{}) (bigtable.GCPolicy, error) {
	var policies []bigtable.GCPolicy

	binding := d
	mode := binding["mode"].(string)
	ma := binding["max_age"].(*schema.Set).List()
	mv := binding["max_version"].(*schema.Set).List()
	gcRules := binding["gc_rules"].(string)

	if len(ma) == 0 && len(mv) == 0 && gcRules == "" {
		fmt.Println("No GC Policy SET")
		return bigtable.NoGcPolicy(), nil
	}

	if mode == "" && len(ma) > 0 && len(mv) > 0 {
		fmt.Println("MODE is empty")
		return nil, fmt.Errorf("if multiple policies are set, mode can't be empty")
	}

	if gcRules != "" {
		fmt.Println("gcRules is not empty")
		var topLevelPolicy map[string]interface{}
		if err := json.Unmarshal([]byte(gcRules), &topLevelPolicy); err != nil {
			return nil, err
		}
		return getGCPolicyFromJSON(topLevelPolicy /*isTopLevel=*/, true)
	}

	if len(ma) > 0 {
		fmt.Println("max_age is not empty")
		l, _ := ma[0].(map[string]interface{})
		fmt.Println("max_age---l :", l)
		d, err := getMaxAgeDuration(l)
		if err != nil {
			return nil, err
		}
		policies = append(policies, bigtable.MaxAgePolicy(d))
	}

	if len(mv) > 0 {
		fmt.Println("max_version is not empty")
		l, _ := mv[0].(map[string]interface{})
		n, _ := l["number"].(int)

		policies = append(policies, bigtable.MaxVersionsPolicy(n))
	}

	switch mode {
	case GCPolicyModeUnion:
		return bigtable.UnionPolicy(policies...), nil
	case GCPolicyModeIntersection:
		return bigtable.IntersectionPolicy(policies...), nil
	}

	return policies[0], nil
}
