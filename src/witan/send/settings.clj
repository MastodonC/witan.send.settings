(ns witan.send.settings
  (:require [clojure.set :as set]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure.string  :as string]
            [tablecloth.api  :as tc]
            [tech.v3.dataset :as ds]
            [witan.gias :as gias]))



;;; # Utility functions
(defn- compare-mapped-keys
  [m k1 k2]
  (compare [(get m k1) k1]
           [(get m k2) k2]))

(defn ds->sorted-map-by
  "Given dataset `ds`, returns sorted-map with
   - keys from the `key-cols` of `ds` (as maps if multiple `key-cols`),
   - vals as maps of the remaining dataset columns keyed by column-name,
   - ordered by the order or rows in the `ds`.
  Optional trailing keyword arguments:
   - `:drop-val-cols` specify columns to drop from vals
     (default `key-cols`, specify a non-existent column to include all).
   - `:order-cols` to specify columns of `ds` to order by."
  [ds key-cols & {:keys [drop-val-cols order-cols]}]
  (let [columns (fn [ds] (if (< 1 (tc/column-count ds))
                           (tc/rows ds :as-maps)
                           (-> (tc/columns ds :as-seqs) first)))
        ks       (-> ds
                     (tc/select-columns key-cols)
                     (columns))
        vs       (-> ds
                     (tc/drop-columns (or drop-val-cols key-cols))
                     (tc/rows :as-maps))
        os       (or
                  (-> ds
                      (tc/select-columns order-cols)
                      (columns))
                  (range))]
    (into (sorted-map-by (partial compare-mapped-keys (zipmap ks os)))
          (zipmap ks vs))
    )
  )

(comment ;; test
  (-> (tc/dataset [{:k1 :k1-1 :k2 :k2-1 :order 2 :v1 1 :v2 2}
                   {:k1 :k1-2 :k2 :k2-2 :order 1 :v1 2 :v2 1}])
      (ds->sorted-map-by [:k1 :k2] #_#_:order-cols :order #_#_:drop-val-cols [:_none_])
      )
  
  )

(defn map->ds
  "Given map with maps as values `m`, returns dataset
   with keys and vals of vals in columns
   named per the keys prefixed with `key-prefix`."
  ;; TODO: Expand to unpack keys that are maps.
  [m key-prefix]
  (->> m
       (reduce-kv (fn [vec-of-rows-as-maps k v]
                    (conj vec-of-rows-as-maps
                          (merge {(keyword key-prefix) k}
                                 (update-keys v (comp keyword (partial str key-prefix "-") name)))))
                  [])
       tc/dataset))



;;; # Setting Definitions
(defn- filepath
  "Parse `filename`, `dir` & `resource-dir` parameters specifying definitions file to read:
   - if `filename` is specified with a path (i.e. contains a \"/\"), then use that filepath, otherwise
   - if `dir` is specified (non-nil), then prepend to `filename` to get filepath, otherwise
   - if `resource-dir` is specified (non-nil), then prepend to `filename` and open resource-dir file on that path,
   - else returns nil."
  [filename dir resource-dir]
  (cond
    (re-find #"\/" filename) filename
    (some? dir)              (str dir filename)
    (some? resource-dir)     (io/resource (str resource-dir filename))
    :else                    nil))

(def csv->ds-opts
  "Common options for `ds/->dataset` read of setting defs CSV files."
  {:file-type        :csv
   :separator        ","
   :header-row?      :true
   :key-fn           keyword
   :parser-fn        {:abbreviation :string
                      :order        :int16
                      :name         :string
                      :label        :string
                      :definition   :string}})

;;; ## `:estab-cat`: Establishment Categories
(defn estab-cats-ds
  "Dataset of establishment category definitions.
   Read from CSV file specified by `::estab-cats-filename`, `::dir` & `::resource-dir` vals,
   unless supplied in truthy `::estab-cats-ds` val."
  [& {estab-cats-ds' ::estab-cats-ds
      ::keys         [estab-cats-filename
                      dir
                      resource-dir]
      :or            {estab-cats-filename "estab-cats.csv"}}]
  (or estab-cats-ds'
      (with-open [in (-> (filepath estab-cats-filename dir resource-dir)
                         io/file
                         io/input-stream)]
        (ds/->dataset in (merge-with merge
                                     csv->ds-opts
                                     {:parser-fn {:designate?  :boolean
                                                  :split-area? :boolean}})))))

(comment ;; test
  (estab-cats-ds ::resource-dir "standard/")
  
  (estab-cats-ds {::estab-cats-filename "estab-cats-test.csv"
                  ::dir                 "./tmp/"})

  (estab-cats-ds {::estab-cats-filename "./tmp/estab-cats-test.csv"
                  ::dir                 "./tmp/"
                  ::resource-dir        "standard/"})

  (estab-cats-ds ::estab-cats-ds "override")

  )

(defn estab-cats
  "Map establishment category abbreviations to attributes.
   Derived from `(estab-cats-ds cfg)` unless specified in truthy `::estab-cats` val."
  [& {estab-cats' ::estab-cats
      :as         cfg}]
  (or estab-cats'
      (-> (estab-cats-ds cfg)
          (ds->sorted-map-by :abbreviation :order-cols :order))))

(comment ;; test
  (estab-cats ::estab-cats-ds (estab-cats-ds {::estab-cats-filename "./tmp/estab-cats-test.csv"}))
  (estab-cats ::estab-cats "override")

  )


;;; ## `:designation`: Designations
(defn designations-ds
  "Dataset of designation definitions.
   Read from CSV file specified by `::designations-filename`, `::dir` & `::resource-dir` vals,
   unless supplied in truthy `::designations-ds` val."
  [& {designations-ds' ::designations-ds
      ::keys           [designations-filename
                        dir
                        resource-dir]
      :or              {designations-filename "designations.csv"}}]
  (or designations-ds'
      (with-open [in (-> (filepath designations-filename dir resource-dir)
                         io/file
                         io/input-stream)]
        (ds/->dataset in csv->ds-opts))))

(comment ;; test
  (designations-ds ::resource-dir "standard/")
  
  (designations-ds {::designations-filename "designations-test.csv"
                    ::dir                   "./tmp/"})

  (designations-ds {::designations-filename "./tmp/designations-test.csv"
                    ::resource-dir          "standard/"})

  )

(defn designations
  "Map designation abbreviations to attributes.
   Derived from `(designations-ds cfg)` unless specified in truthy `::designations` val."
  [& {designations' ::designations
      :as           cfg}]
  (or designations'
      (-> (designations-ds cfg)
          (ds->sorted-map-by :abbreviation :order-cols :order))))

(comment ;; test
  (designations ::designations-ds (designations-ds {::designations-filename "./tmp/designations-test.csv"}))

  )


;;; ## `:area`: Area
(defn areas-ds
  "Dataset of area definitions.
   Read from CSV file specified by `::areas-filename`, `::dir` & `::resource-dir` vals,
   unless supplied in truthy `::areas-ds` val."
  [& {areas-ds' ::areas-ds
      ::keys    [areas-filename
                 dir
                 resource-dir]
      :or       {areas-filename "areas.csv"}}]
  (or areas-ds'
      (with-open [in (-> (filepath areas-filename dir resource-dir)
                         io/file
                         io/input-stream)]
        (ds/->dataset in csv->ds-opts))))

(comment ;; test
  (areas-ds ::resource-dir "standard/")
  
  (areas-ds {::areas-filename "areas-test.csv"
             ::dir            "./tmp/"})

  (areas-ds {::areas-filename "./tmp/areas-test.csv"
             ::resource-dir   "standard/"})

  )


(defn areas
  "Map area abbreviations to attributes.
  Derived from `(areas-ds cfg)` unless specified in truthy `::areas` val."
  [& {areas' ::areas
      :as    cfg}]
  (or areas'
      (-> (areas-ds cfg)
          (ds->sorted-map-by :abbreviation :order-cols :order))))

(comment ;; test
  (areas ::areas-ds (areas-ds {::areas-filename "./tmp/areas-test.csv"}))

  )




;;; ## Settings
;; Combining `:estab-cat`, `:designation` and `:area`:
(defn settings
  "Map setting abbreviations to attributes.
   Derived from estab-cat, designation & area definitions unless specified in truthy `::settings` val:
   - estab-cat definitions from `(estab-cats cfg)`
   - designation definitions from `(designations cfg)`
   - area definitions from `(areas cfg)`"
  [& {settings' ::settings
      :as       cfg}]
  (or settings'
      (let [;; Derive definition datasets from maps in case maps have been provided in `cfg`
            ;; TODO: If maps not provided then use ds directly? Move map->ds to the ds fns if map provided?
            estab-cats'      (estab-cats cfg)
            designations'    (designations cfg)
            areas'           (areas cfg)
            estab-cats-ds'   (map->ds estab-cats' "estab-cat")
            designations-ds' (map->ds designations' "designation")
            areas-ds'        (map->ds areas' "area")]
        ;; TODO: Ensure works when not designating or area-splitting.
        ;; TODO: Only construct name|label|definition if have all components needed (i.e. need area if :estab-cat-split-area).
        (-> estab-cats-ds'
            ;; Expand `:estab-cat`s to be designated by designations
            (as-> $
                (tc/concat-copying (tc/drop-rows $ (comp :estab-cat-designate?))
                                   (-> (tc/select-rows $ (comp :estab-cat-designate?))
                                       (tc/cross-join designations-ds'))))
            ;; Expand out `:estab-cat`s to be split by area
            (as-> $
                (tc/concat-copying (tc/drop-rows $ (comp :estab-cat-split-area?))
                                   (-> (tc/select-rows $ (comp :estab-cat-split-area?))
                                       (tc/cross-join areas-ds'))))
            ;; Derive setting `:abbreviation`
            (tc/map-columns :abbreviation [:estab-cat :designation :area]
                            (fn [& args] (clojure.string/join "_" (filter some? args))))
            (tc/order-by [:estab-cat-order :designation-order :area-order])
            (tc/add-column :order (iterate inc 1))
            ;; Derive setting attributes from corresponding estab-cat, designation & area attributes (if available)
            (tc/map-rows (fn [{:keys [estab-cat-name       designation-name       area-name
                                      estab-cat-label      designation-label      area-label
                                      estab-cat-definition designation-definition area-definition]}]
                           (let [setting-attr-f (fn [estab-cat-attr designation-attr area-attr]
                                                  (str estab-cat-attr
                                                       (when designation-attr (format " - %s" designation-label))
                                                       (when area-attr (format " [%s]" area-label))))]
                             (merge (when estab-cat-name
                                      {:name       (setting-attr-f estab-cat-name       designation-name       area-name      )})
                                    (when estab-cat-label
                                      {:label      (setting-attr-f estab-cat-label      designation-label      area-label     )})
                                    (when estab-cat-definition
                                      {:definition (setting-attr-f estab-cat-definition designation-definition area-definition)})))))
            ;; Tidy dataset
            (tc/select-columns [:abbreviation :order :name :label :definition :estab-cat :designation :area])
            ;; Convert to map
            (ds->sorted-map-by :abbreviation :order-cols :order)))))

(comment ;; test
  (-> {::resource-dir "standard/"}
      ((fn [m] (assoc m
                      ::estab-cats-ds (-> (estab-cats-ds m)
                                          (tc/drop-columns [:name]))
                      #_#_::designations-ds (-> (designations-ds m)
                                                (tc/drop-columns [:name :order]))
                      #_#_::areas-ds (-> (areas-ds m)
                                         (tc/drop-columns [:name :order]))
                      )))
      (settings)
      #_(tc/drop-columns #"^.*definition")
      #_(vary-meta assoc :print-index-range 1000)
      )

  )


(defn setting-split-regexp
  "Return regex pattern for splitting setting abbreviations into components.
   Relies on area abbreviations not clashing with designation abbreviations.
   Collection of areas can be specified (first supplied is used):
   - directly via `:area-abbreviations` key
   - via keys of areas map specified in `:areas` key
   or (if not) is via keys of areas map derived from settings cfg via `(areas cfg)`."
  [& {setting-split-regex' ::setting-split-regex
      area-abbreviations   :area-abbreviations ; Note deliberately NOT namespaced
      areas'               ::areas
      :as                  cfg}]
  (or setting-split-regex'
      (let [area-abbreviations' (or area-abbreviations
                                    (keys (or areas'
                                              (areas cfg))))]
        (re-pattern (str "^(?<estabCat>[^_]+)"
                         "_??(?<designation>[^_]+)??"
                         "_?(?<area>" (string/join "|" area-abbreviations') ")?$")))))

(comment ;; test
  (setting-split-regexp ::setting-split-regex "42")
  (setting-split-regexp :area-abbreviations ["foo" "bar"])
  (setting-split-regexp ::resource-dir "standard/")
  (-> (settings ::resource-dir "standard/")
      keys
      (as-> $ (tc/dataset {:setting $}))
      (tc/separate-column :setting [:estab-cat :designation :area-indicator]
                          (setting-split-regexp ::resource-dir "standard/")
                          {:drop-column? false})
      (vary-meta assoc :print-index-range 1000)
      )

  )
