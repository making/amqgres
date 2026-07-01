package com.example.amqgres;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

/**
 * Enforces the package architecture: the feature slices must stay free of cycles, and the
 * {@code config} package is the composition root, so nothing outside it may depend on it.
 */
@AnalyzeClasses(packages = "com.example.amqgres", importOptions = ImportOption.DoNotIncludeTests.class)
class PackageArchitectureTest {

	@ArchTest
	static final ArchRule featurePackagesShouldBeFreeOfCycles = slices().matching("com.example.amqgres.(*)..")
		.should()
		.beFreeOfCycles();

	@ArchTest
	static final ArchRule nothingShouldDependOnConfig = noClasses().that()
		.resideOutsideOfPackage("..config..")
		.should()
		.dependOnClassesThat()
		.resideInAPackage("..config..")
		.because("config is the composition root; feature packages must not depend on it");

}
